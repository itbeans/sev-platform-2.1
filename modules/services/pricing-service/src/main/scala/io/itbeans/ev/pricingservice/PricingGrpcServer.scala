package io.itbeans.ev.pricingservice

import io.circe.parser.decode
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// PricingGrpcServer — pre-codegen ADTs and handler for the Pricing gRPC API.
//
// When ScalaPB code generation is wired up, these ADTs will be replaced by
// the generated protobuf message classes from pricing_service.proto.
// The handler class below is intentionally kept free of gRPC transport so
// it can be unit-tested without a running gRPC server.
//
// gRPC contract (from pricing_service.proto):
//   ResolvePricing     — tariff selection at session start
//   PriceConsumption   — price one MeterValues interval
//   FinalisePrice      — compute final session price at transaction stop
// ---------------------------------------------------------------------------

// ── Pre-codegen request/response ADTs ────────────────────────────────────

case class ResolvePricingRequestADT(
    tenantId: String,
    chargingStationId: String,
    connectorId: String,
    connectorType: String,
    connectorPowerKw: Double,
    siteId: Option[String],
    siteAreaId: Option[String],
    companyId: Option[String],
    userId: String,
    startTimestamp: Long // Unix epoch millis
)

case class ResolvePricingResponseADT(
    found: Boolean,
    pricingModelJson: String, // JSON-encoded PricingDefinition (empty if !found)
    currency: String
)

case class PriceConsumptionRequestADT(
    tenantId: String,
    transactionId: String,
    pricingModelJson: String, // serialized PricingDefinition from ResolvePricing
    consumptionWh: Double,
    cumulatedWh: Double,
    totalDurationSecs: Int,
    inactivitySecs: Int,
    totalInactivitySecs: Int,
    intervalStart: Long, // Unix epoch millis
    intervalEnd: Long,
    timezone: String,
    connectorType: String,
    connectorPowerKw: Double,
    flatFeeAlreadyPriced: Boolean,
    cumulatedPrice: Double
)

case class PriceConsumptionResponseADT(
    price: Double,
    cumulatedPrice: Double,
    currency: String,
    flatFeeAlreadyPriced: Boolean
)

case class FinalisePriceRequestADT(
    tenantId: String,
    transactionId: String,
    pricingModelJson: String,
    totalConsumptionWh: Double,
    startTimestamp: Long,
    endTimestamp: Long,
    timezone: String,
    connectorType: String,
    connectorPowerKw: Double
)

case class FinalisePriceResponseADT(
    totalPrice: Double,
    currency: String,
    invoiceItem: String // JSON-encoded invoice line item for the Billing service
)

// ── Handler ───────────────────────────────────────────────────────────────

final class PricingGrpcHandler(
    repo: PricingRepository,
    resolver: TariffResolver,
    pricer: ConsumptionPricer
):

  /**
   * Resolve the applicable pricing model for a new charging session.
   * Called by ev-ocpp-processor at transaction start.
   */
  def resolvePricing(req: ResolvePricingRequestADT): Task[ResolvePricingResponseADT] =
    val ctx = TariffResolver.ResolutionContext(
      chargingStationId = req.chargingStationId,
      siteAreaId = req.siteAreaId,
      siteId = req.siteId,
      companyId = req.companyId,
      tenantId = TenantId(req.tenantId),
      connectorType = req.connectorType,
      connectorPowerKw = req.connectorPowerKw,
      timestamp = Instant.ofEpochMilli(req.startTimestamp)
    )
    for
      defOpt <- resolver.resolve(ctx)
      _ <- ZIO.logInfo(
        s"ResolvePricing tenant=${req.tenantId} " +
          s"station=${req.chargingStationId} " +
          s"found=${defOpt.isDefined}"
      )
    yield defOpt match
      case None =>
        ResolvePricingResponseADT(found = false, pricingModelJson = "", currency = "EUR")
      case Some(d) =>
        ResolvePricingResponseADT(
          found = true,
          pricingModelJson = d.asJson.noSpaces,
          currency = d.currencyCode
        )

  /**
   * Price one MeterValues consumption interval.
   * Called by ev-ocpp-processor on each MeterValues message.
   */
  def priceConsumption(req: PriceConsumptionRequestADT): Task[PriceConsumptionResponseADT] =
    for
      definition <- parseDefinition(req.pricingModelJson)
      interval = ConsumptionInterval(
        intervalStart = Instant.ofEpochMilli(req.intervalStart),
        intervalEnd = Instant.ofEpochMilli(req.intervalEnd),
        consumptionWh = req.consumptionWh,
        cumulatedConsumptionWh = req.cumulatedWh,
        totalDurationSecs = req.totalDurationSecs,
        inactivitySecs = req.inactivitySecs,
        totalInactivitySecs = req.totalInactivitySecs,
        timezone = req.timezone,
        connectorType = req.connectorType,
        connectorPowerKw = req.connectorPowerKw
      )
      state = PricerState(
        flatFeeAlreadyPriced = req.flatFeeAlreadyPriced,
        cumulatedAmount = BigDecimal(req.cumulatedPrice),
        currencyCode = definition.currencyCode
      )
      (priced, newState) = pricer.priceInterval(definition, interval, state)
      _ <- ZIO.logDebug(
        s"PriceConsumption tx=${req.transactionId} " +
          s"intervalAmount=${priced.intervalAmount} " +
          s"cumulated=${priced.cumulatedAmount}"
      )
    yield PriceConsumptionResponseADT(
      price = priced.intervalAmount.toDouble,
      cumulatedPrice = priced.cumulatedAmount.toDouble,
      currency = priced.currencyCode,
      flatFeeAlreadyPriced = newState.flatFeeAlreadyPriced
    )

  /**
   * Finalise pricing at transaction stop.
   * Computes the final session price from the total consumption and duration.
   */
  def finalisePrice(req: FinalisePriceRequestADT): Task[FinalisePriceResponseADT] =
    for
      definition <- parseDefinition(req.pricingModelJson)
      totalSecs = ((req.endTimestamp - req.startTimestamp) / 1000).toInt.max(0)
      // Create a single interval spanning the full session for final pricing
      interval = ConsumptionInterval(
        intervalStart = Instant.ofEpochMilli(req.startTimestamp),
        intervalEnd = Instant.ofEpochMilli(req.endTimestamp),
        consumptionWh = req.totalConsumptionWh,
        cumulatedConsumptionWh = req.totalConsumptionWh,
        totalDurationSecs = totalSecs,
        inactivitySecs = 0,
        totalInactivitySecs = 0,
        timezone = req.timezone,
        connectorType = req.connectorType,
        connectorPowerKw = req.connectorPowerKw
      )
      state       = PricerState.initial(definition.currencyCode)
      (priced, _) = pricer.priceInterval(definition, interval, state)
      invoiceItem = buildInvoiceItem(definition, priced)
      _ <- ZIO.logInfo(
        s"FinalisePrice tx=${req.transactionId} " +
          s"total=${priced.intervalAmount} ${definition.currencyCode}"
      )
    yield FinalisePriceResponseADT(
      totalPrice = priced.intervalAmount.toDouble,
      currency = definition.currencyCode,
      invoiceItem = invoiceItem
    )

  // ── Helpers ──────────────────────────────────────────────────────────────

  private def parseDefinition(json: String): Task[PricingDefinition] =
    if json.isBlank then
      ZIO.fail(new IllegalArgumentException("pricingModelJson is empty — session has no tariff"))
    else
      ZIO.fromEither(decode[PricingDefinition](json))
        .mapError(e => new IllegalArgumentException(s"Failed to parse pricingModelJson: ${e.getMessage}"))

  private def buildInvoiceItem(d: PricingDefinition, priced: PricedInterval): String =
    import io.circe.Json
    Json.obj(
      "tariffName"  -> Json.fromString(d.name),
      "currency"    -> Json.fromString(d.currencyCode),
      "totalAmount" -> Json.fromBigDecimal(priced.intervalAmount),
      "dimensions" -> Json.obj(
        "flatFee"      -> priced.flatFee.map(r => Json.fromBigDecimal(r.amount)).getOrElse(Json.Null),
        "energy"       -> priced.energy.map(r => Json.fromBigDecimal(r.amount)).getOrElse(Json.Null),
        "chargingTime" -> priced.chargingTime.map(r => Json.fromBigDecimal(r.amount)).getOrElse(Json.Null),
        "parkingTime"  -> priced.parkingTime.map(r => Json.fromBigDecimal(r.amount)).getOrElse(Json.Null)
      )
    ).noSpaces

object PricingGrpcHandler:

  val live: ZLayer[PricingRepository & TariffResolver & ConsumptionPricer, Nothing, PricingGrpcHandler] =
    ZLayer.fromFunction(new PricingGrpcHandler(_, _, _))
