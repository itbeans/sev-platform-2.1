package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}

opaque type TransactionId = Long

object TransactionId:
  def apply(v: Long): TransactionId             = v
  def unapply(id: TransactionId): Some[Long]    = Some(id)
  extension (id: TransactionId) def value: Long = id

  given Schema[TransactionId] =
    Schema.Primitive(StandardType.LongType, zio.Chunk.empty).transform(TransactionId(_), _.value)

enum TransactionAction:
  case Start, Update, Stop

enum TransactionStop:

  case EVDisconnected, Local, Remote, DeAuthorized, EmergencyStop,
    PowerLoss, Reboot, Other, EVSEError, GroundFault, OverCurrentFault,
    SOCLimitReached, Timeout

case class Transaction(
    id: TransactionId,
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    siteId: Option[SiteId],
    siteAreaId: Option[SiteAreaId],
    companyId: Option[CompanyId],
    connectorId: Int,
    // OCPP 2.1: evseId replaces connectorId in v2.x
    evseId: Option[Int],
    tagId: Option[String],
    userId: Option[UserId],
    carId: Option[String],
    ocppVersion: OcppVersion,
    // Timing
    timestamp: java.time.Instant,
    startDate: java.time.Instant,
    endDate: Option[java.time.Instant],
    // Energy
    meterStart: Double,
    meterStop: Option[Double],
    currentInstantWatts: Option[Double],
    currentStateOfCharge: Option[Int],
    currentTotalDurationSecs: Option[Long],
    currentTotalInactivitySecs: Option[Long],
    currentConsumptionWh: Option[Double],
    // Billing
    priceUnit: Option[String],
    currentCumulatedPrice: Option[Double],
    // Stop reason
    stopReason: Option[TransactionStop],
    stoppedBy: Option[UserId],
    // OCPP 2.1: transaction can be resumed after forced reboot
    resumable: Boolean = false,
    // OCPP 2.1: fixed cost/energy/time limit
    limitType: Option[TransactionLimitType] = None,
    limitValue: Option[Double] = None,
    // Status
    inProgress: Boolean,
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

enum TransactionLimitType:
  case Energy   // kWh limit
  case Cost     // currency limit
  case Duration // time limit in seconds

// A single sampled value from MeterValues
case class Consumption(
    transactionId: TransactionId,
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    connectorId: Int,
    startedAt: java.time.Instant,
    endedAt: java.time.Instant,
    consumptionWh: Double,
    instantWatts: Option[Double],
    stateOfCharge: Option[Int],
    pricingSource: Option[String],
    cumulatedPrice: Option[Double]
)

object Transaction:
  given Schema[TransactionStop] = DeriveSchema.gen
  given Schema[TransactionAction] = DeriveSchema.gen
  given Schema[TransactionLimitType] = DeriveSchema.gen
  given Schema[Transaction] = DeriveSchema.gen
  given Schema[Consumption] = DeriveSchema.gen
