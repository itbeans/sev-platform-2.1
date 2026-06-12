package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type CarId = String

object CarId:
  def apply(v: String): CarId             = v
  def unapply(id: CarId): Some[String]    = Some(id)
  extension (id: CarId) def value: String = id
  given Schema[CarId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(CarId(_), _.value)

// ---------------------------------------------------------------------------
// Car domain — user EVs and the global car catalog (sourced from EV Database).
// Mirrors the TypeScript Car / CarCatalog storage model.
// ---------------------------------------------------------------------------

enum CarType(val code: String):
  case Private extends CarType("P")
  case Company extends CarType("C")
  case PoolCar extends CarType("PC")

// Whether a car converter (on-board charger) came as standard, optional, or alternative
enum ConverterType(val code: String):
  case Standard    extends ConverterType("S")
  case Optional    extends ConverterType("O")
  case Alternative extends ConverterType("A")

case class CarConverter(
    powerWatts: Double,
    amperagePerPhase: Double,
    numberOfPhases: Int,
    converterType: ConverterType
)

// External connector integration (e.g. Mercedes, Targa, Tronity)
case class CarConnectorData(
    carConnectorId: String,
    carConnectorMeterId: Option[String]
)

case class Car(
    id: CarId,
    tenantId: TenantId,
    userId: UserId,
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Int,
    carType: CarType,
    default: Boolean,
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData],
    createdOn: Instant,
    lastChangedOn: Instant
)

// Read-only catalog entry sourced from EV Database API
case class CarCatalog(
    id: Int,
    vehicleMake: String,
    vehicleModel: String,
    vehicleModelVersion: Option[String],
    drivetrainPropulsion: Option[String],
    drivetrainPowerHP: Option[Int],
    performanceAcceleration: Option[Double],
    performanceTopspeed: Option[Int],
    rangeReal: Option[Int],
    rangeWLTP: Option[Int],
    efficiencyReal: Option[Double],
    batteryCapacityUseable: Option[Double],
    batteryCapacityFull: Option[Double],
    chargePlug: Option[String],
    chargeStandardPower: Option[Double],
    chargeStandardPhase: Option[Int],
    chargeStandardPhaseAmp: Option[Double],
    chargeStandardChargeTime: Option[Int],
    chargeStandardChargeSpeed: Option[Double],
    fastChargePlug: Option[String],
    fastChargePowerMax: Option[Double],
    miscBody: Option[String],
    miscSegment: Option[String],
    miscSeats: Option[Int],
    imageUrls: List[String],
    image: Option[String],
    hash: Option[String],
    imagesHash: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

object Car:
  given Schema[CarType] = DeriveSchema.gen
  given Schema[ConverterType] = DeriveSchema.gen
  given Schema[CarConverter] = DeriveSchema.gen
  given Schema[CarConnectorData] = DeriveSchema.gen
  given Schema[Car] = DeriveSchema.gen
  given Schema[CarCatalog] = DeriveSchema.gen
