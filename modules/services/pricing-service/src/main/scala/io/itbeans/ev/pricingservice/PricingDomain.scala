package io.itbeans.ev.pricingservice

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

import java.time.Instant

// Circe instances for types that lack built-in support.
private[pricingservice] object CirceInstances:
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    scala.util.Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  }
  given Encoder[BigDecimal] = Encoder.encodeString.contramap(_.toString)

  given Decoder[BigDecimal] = Decoder.decodeString.emap { s =>
    scala.util.Try(BigDecimal(s)).toEither.left.map(_.getMessage)
  }

import CirceInstances.given

// ---------------------------------------------------------------------------
// Pricing domain — tariff definitions and session pricing types.
//
// PricingDefinition:
//   Stores one tariff attached to an entity in the entity hierarchy.
//   Hierarchy (most → least specific): ChargingStation → Site → Tenant
//   First matching definition wins per consumption chunk.
//
// Pricing flow:
//   1. TariffResolver selects the applicable PricingDefinition for a session
//      (static restrictions: date range, connector type, connector power)
//   2. ConsumptionPricer applies active dimensions to each consumption interval
//      (dynamic restrictions: day-of-week, time-of-day, energy/duration bounds)
// ---------------------------------------------------------------------------

/** Entity hierarchy level that owns a PricingDefinition. */
enum PricingEntity(val code: String):
  case Tenant          extends PricingEntity("T")
  case Company         extends PricingEntity("C")
  case Site            extends PricingEntity("S")
  case SiteArea        extends PricingEntity("SA")
  case ChargingStation extends PricingEntity("CS")

object PricingEntity:
  def fromCode(code: String): Option[PricingEntity] = values.find(_.code == code)
  given Encoder[PricingEntity] = Encoder.encodeString.contramap(_.code)

  given Decoder[PricingEntity] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown PricingEntity code: $s")
  )

/** ISO 8601 day-of-week: 1 = Monday … 7 = Sunday */
enum DayOfWeek(val isoValue: Int):
  case Monday    extends DayOfWeek(1)
  case Tuesday   extends DayOfWeek(2)
  case Wednesday extends DayOfWeek(3)
  case Thursday  extends DayOfWeek(4)
  case Friday    extends DayOfWeek(5)
  case Saturday  extends DayOfWeek(6)
  case Sunday    extends DayOfWeek(7)

object DayOfWeek:
  def fromIso(n: Int): Option[DayOfWeek] = values.find(_.isoValue == n)
  given Encoder[DayOfWeek] = Encoder.encodeInt.contramap(_.isoValue)

  given Decoder[DayOfWeek] = Decoder.decodeInt.emap(n =>
    fromIso(n).toRight(s"Unknown DayOfWeek ISO value: $n")
  )

/** A single pricing dimension (flat fee / energy / charging time / parking time). */
case class PricingDimension(
    active: Boolean,
    price: BigDecimal,    // unit price excluding VAT
    stepSize: Option[Int] // Wh for energy; seconds for time dimensions
)

object PricingDimension:
  given Encoder[PricingDimension] = deriveEncoder
  given Decoder[PricingDimension] = deriveDecoder

/** The full set of pricing dimensions for one tariff. */
case class PricingDimensions(
    flatFee: Option[PricingDimension],
    energy: Option[PricingDimension],
    chargingTime: Option[PricingDimension],
    parkingTime: Option[PricingDimension]
)

object PricingDimensions:
  given Encoder[PricingDimensions] = deriveEncoder
  given Decoder[PricingDimensions] = deriveDecoder

/** Restrictions evaluated once at session start (tariff candidate filtering). */
case class PricingStaticRestriction(
    validFrom: Option[Instant],
    validTo: Option[Instant],
    connectorType: Option[String],
    connectorPowerKw: Option[Double]
)

object PricingStaticRestriction:
  given Encoder[PricingStaticRestriction] = deriveEncoder
  given Decoder[PricingStaticRestriction] = deriveDecoder

/** Restrictions evaluated per consumption chunk (time-of-day, energy bounds, etc). */
case class PricingRestriction(
    daysOfWeek: List[DayOfWeek],
    timeFrom: Option[String], // "HH:mm" in station timezone
    timeTo: Option[String],   // "HH:mm" — may cross midnight (e.g., "20:00"–"08:00")
    minEnergyKwh: Option[Double],
    maxEnergyKwh: Option[Double],
    minDurationSecs: Option[Int],
    maxDurationSecs: Option[Int]
)

object PricingRestriction:
  given Encoder[PricingRestriction] = deriveEncoder
  given Decoder[PricingRestriction] = deriveDecoder

/** A complete tariff definition stored in `{tenantId}.pricingdefinitions`. */
case class PricingDefinition(
    id: String,
    entityId: String,
    entityType: PricingEntity,
    name: String,
    description: Option[String],
    siteId: Option[String],
    dimensions: PricingDimensions,
    staticRestrictions: Option[PricingStaticRestriction],
    restrictions: Option[PricingRestriction],
    currencyCode: String,
    createdOn: Instant,
    lastChangedOn: Instant
)

object PricingDefinition:
  given Encoder[PricingDefinition] = deriveEncoder
  given Decoder[PricingDefinition] = deriveDecoder

// ---------------------------------------------------------------------------
// Consumption pricing runtime types
// ---------------------------------------------------------------------------

/** One metering interval to price (corresponds to one MeterValues event). */
case class ConsumptionInterval(
    intervalStart: Instant,
    intervalEnd: Instant,
    consumptionWh: Double,          // energy delivered in this interval
    cumulatedConsumptionWh: Double, // total energy since session start
    totalDurationSecs: Int,         // total session duration so far (secs)
    inactivitySecs: Int,            // idle seconds within this interval
    totalInactivitySecs: Int,       // total idle since session start (secs)
    timezone: String,               // IANA, e.g. "Europe/Paris"
    connectorType: String,          // e.g. "Type2", "CCS"
    connectorPowerKw: Double        // connector rated power in kW
)

/** Session pricing state threaded across consecutive interval calls. */
case class PricerState(
    flatFeeAlreadyPriced: Boolean,
    cumulatedAmount: BigDecimal,
    currencyCode: String
)

object PricerState:

  def initial(currencyCode: String): PricerState =
    PricerState(flatFeeAlreadyPriced = false, cumulatedAmount = BigDecimal(0), currencyCode = currencyCode)

/** Pricing result for one dimension. */
case class DimensionResult(
    amount: BigDecimal,
    roundedAmount: BigDecimal,
    quantity: Double, // Wh, seconds, or 1 (flat fee)
    unitPrice: BigDecimal,
    stepSize: Option[Int]
)

/** Complete pricing result for one consumption interval. */
case class PricedInterval(
    intervalAmount: BigDecimal,
    cumulatedAmount: BigDecimal,
    currencyCode: String,
    flatFee: Option[DimensionResult],
    energy: Option[DimensionResult],
    chargingTime: Option[DimensionResult],
    parkingTime: Option[DimensionResult]
)

object PricedInterval:
  given Encoder[DimensionResult] = deriveEncoder
  given Encoder[PricedInterval] = deriveEncoder
