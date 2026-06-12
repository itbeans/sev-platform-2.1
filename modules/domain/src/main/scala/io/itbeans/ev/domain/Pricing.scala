package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type PricingId = String

object PricingId:
  def apply(v: String): PricingId             = v
  def unapply(id: PricingId): Some[String]    = Some(id)
  extension (id: PricingId) def value: String = id
  given Schema[PricingId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(PricingId(_), _.value)

// ---------------------------------------------------------------------------
// Pricing domain — tariff definitions, per-dimension prices, restrictions.
// Mirrors the TypeScript PricingDefinition / PricingRestriction storage model.
// ---------------------------------------------------------------------------

// The level at which a pricing rule is attached
enum PricingEntity(val code: String):
  case Tenant               extends PricingEntity("T")
  case Company              extends PricingEntity("C")
  case Site                 extends PricingEntity("S")
  case SiteArea             extends PricingEntity("SA")
  case ChargingStationLevel extends PricingEntity("CS")

enum PricingDayOfWeek(val isoValue: Int):
  case Monday    extends PricingDayOfWeek(1)
  case Tuesday   extends PricingDayOfWeek(2)
  case Wednesday extends PricingDayOfWeek(3)
  case Thursday  extends PricingDayOfWeek(4)
  case Friday    extends PricingDayOfWeek(5)
  case Saturday  extends PricingDayOfWeek(6)
  case Sunday    extends PricingDayOfWeek(7)

// One billable dimension (flat fee, energy, charging time, parking time)
case class PricingDimension(
    active: Boolean,
    price: BigDecimal,
    stepSize: Option[Int]
)

case class PricingDimensions(
    flatFee: Option[PricingDimension],
    energy: Option[PricingDimension],
    chargingTime: Option[PricingDimension],
    parkingTime: Option[PricingDimension]
)

// Restrictions that narrow when a tariff applies (connector type, valid window)
case class PricingStaticRestriction(
    validFrom: Option[Instant],
    validTo: Option[Instant],
    connectorType: Option[String],
    connectorPowerKw: Option[Double]
)

// Dynamic restrictions per session (day-of-week, time-of-day, energy thresholds)
case class PricingRestriction(
    daysOfWeek: List[PricingDayOfWeek],
    timeFrom: Option[String], // "HH:mm"
    timeTo: Option[String],   // "HH:mm"
    minEnergyKwh: Option[Double],
    maxEnergyKwh: Option[Double],
    minDurationSecs: Option[Int],
    maxDurationSecs: Option[Int]
)

case class PricingDefinition(
    id: PricingId,
    tenantId: TenantId,
    entityId: String,
    entityType: PricingEntity,
    name: String,
    description: Option[String],
    siteId: Option[SiteId],
    dimensions: PricingDimensions,
    staticRestrictions: Option[PricingStaticRestriction],
    restrictions: Option[PricingRestriction],
    currencyCode: String,
    createdOn: Instant,
    lastChangedOn: Instant
)

object Pricing:
  given Schema[PricingEntity] = DeriveSchema.gen
  given Schema[PricingDayOfWeek] = DeriveSchema.gen
  given Schema[PricingDimension] = DeriveSchema.gen
  given Schema[PricingDimensions] = DeriveSchema.gen
  given Schema[PricingStaticRestriction] = DeriveSchema.gen
  given Schema[PricingRestriction] = DeriveSchema.gen
  given Schema[PricingDefinition] = DeriveSchema.gen
