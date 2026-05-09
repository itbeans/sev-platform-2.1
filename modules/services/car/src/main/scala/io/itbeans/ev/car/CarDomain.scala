package io.itbeans.ev.car

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import io.itbeans.ev.domain.{TenantId, UserId}

import java.time.Instant

// Circe codecs for types without built-in derivation support.
// Opaque types need explicit codecs since circe can't see through the opaqueness.
private[car] object CirceInstances:
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    scala.util.Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  }
  given Encoder[UserId] = Encoder.encodeString.contramap(_.value)
  given Decoder[UserId] = Decoder.decodeString.map(UserId(_))

import CirceInstances.given

// ---------------------------------------------------------------------------
// Car domain — data types for Car and CarCatalog entities.
//
// CarCatalog:
//   Sourced from EVDatabase API, keyed by numeric ID.
//   Stored in the DEFAULT tenant's carcatalogs collection (not tenant-scoped).
//
// Car:
//   User-owned car referencing a CarCatalog. Stored per-tenant.
//   Supports car connectors (Mercedes, Tronity, Targa Telematics) for live SoC.
// ---------------------------------------------------------------------------

/** Type of car ownership */
enum CarType(val code: String):
  case Private extends CarType("P")
  case Company extends CarType("C")
  case PoolCar extends CarType("PC")

object CarType:

  def fromCode(code: String): Option[CarType] =
    values.find(_.code == code)

  given Encoder[CarType] = Encoder.encodeString.contramap(_.code)

  given Decoder[CarType] = Decoder.decodeString.emap { s =>
    fromCode(s).toRight(s"Unknown CarType code: $s")
  }

/** AC/DC charger converter variant */
enum ConverterType(val code: String):
  case Standard    extends ConverterType("S")
  case Option      extends ConverterType("O")
  case Alternative extends ConverterType("A")

object ConverterType:

  def fromCode(code: String): Option[ConverterType] =
    values.find(_.code == code)

  given Encoder[ConverterType] = Encoder.encodeString.contramap(_.code)

  given Decoder[ConverterType] = Decoder.decodeString.emap { s =>
    fromCode(s).toRight(s"Unknown ConverterType code: $s")
  }

/** Optional AC converter spec — some vehicles support multiple charger variants */
case class CarConverter(
    powerWatts: Double,
    amperagePerPhase: Double,
    numberOfPhases: Int,
    converterType: ConverterType
)

object CarConverter:
  given Encoder[CarConverter] = deriveEncoder
  given Decoder[CarConverter] = deriveDecoder

/** Link to a third-party car telemetry connector */
case class CarConnectorData(
    carConnectorId: String,             // "mercedes" | "tronity" | "targaTelematics"
    carConnectorMeterId: Option[String] // third-party vehicle ID (required for Tronity)
)

object CarConnectorData:
  given Encoder[CarConnectorData] = deriveEncoder
  given Decoder[CarConnectorData] = deriveDecoder

/** User-owned car record (tenant-scoped collection: {tenantId}.cars) */
case class Car(
    id: String,
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Int,
    userId: UserId,
    carType: CarType,
    default: Boolean,
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData],
    createdOn: Instant,
    lastChangedOn: Instant
)

object Car:
  given Encoder[Car] = deriveEncoder
  given Decoder[Car] = deriveDecoder

/** Vehicle specification from EVDatabase (default-tenant collection: carcatalogs) */
case class CarCatalog(
    id: Int,
    vehicleMake: String,
    vehicleModel: String,
    vehicleModelVersion: Option[String],
    drivetrainPropulsion: Option[String],
    drivetrainPowerHP: Option[Int],
    performanceAcceleration: Option[Double], // 0-100 km/h seconds
    performanceTopspeed: Option[Int],        // km/h
    rangeReal: Option[Int],                  // km
    rangeWLTP: Option[Int],                  // km
    efficiencyReal: Option[Double],          // Wh/km
    batteryCapacityUseable: Option[Double],  // kWh
    batteryCapacityFull: Option[Double],     // kWh
    chargePlug: Option[String],
    chargeStandardPower: Option[Double], // kW
    chargeStandardPhase: Option[Int],
    chargeStandardPhaseAmp: Option[Double],    // A
    chargeStandardChargeTime: Option[Int],     // minutes
    chargeStandardChargeSpeed: Option[Double], // km/h
    fastChargePlug: Option[String],
    fastChargePowerMax: Option[Double], // kW
    miscBody: Option[String],
    miscSegment: Option[String],
    miscSeats: Option[Int],
    imageUrls: List[String],
    image: Option[String],      // base64 thumbnail
    hash: Option[String],       // SHA-256 of catalog JSON
    imagesHash: Option[String], // SHA-256 of imageUrls
    createdOn: Instant,
    lastChangedOn: Instant
)

object CarCatalog:
  given Encoder[CarCatalog] = deriveEncoder
  given Decoder[CarCatalog] = deriveDecoder

/** Lightweight catalog view for list endpoints */
case class CarCatalogSummary(
    id: Int,
    vehicleMake: String,
    vehicleModel: String,
    rangeReal: Option[Int],
    batteryCapacity: Option[Double],
    chargeStandardPower: Option[Double],
    fastChargePowerMax: Option[Double],
    image: Option[String]
)

object CarCatalogSummary:
  given Encoder[CarCatalogSummary] = deriveEncoder
  given Decoder[CarCatalogSummary] = deriveDecoder

  def from(c: CarCatalog): CarCatalogSummary =
    CarCatalogSummary(
      id = c.id,
      vehicleMake = c.vehicleMake,
      vehicleModel = c.vehicleModel,
      rangeReal = c.rangeReal,
      batteryCapacity = c.batteryCapacityUseable,
      chargeStandardPower = c.chargeStandardPower,
      fastChargePowerMax = c.fastChargePowerMax,
      image = c.image
    )

/** Sync result counters */
case class SyncResult(inSuccess: Int, inError: Int)
