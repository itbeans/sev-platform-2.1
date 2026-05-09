package io.itbeans.ev.asset

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

import java.time.Instant

// Circe instances for types without built-in support.
private[asset] object CirceInstances:
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    scala.util.Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  }

import CirceInstances.given

// ---------------------------------------------------------------------------
// Asset domain — data types for external energy asset integration.
//
// Assets represent non-EV energy devices connected to a site:
//   - CONSUMPTION: building loads, HVAC, etc.
//   - PRODUCTION: solar panels, wind generators
//   - CONSUMPTION_AND_PRODUCTION: battery systems (charge + discharge)
//
// Consumption data is polled from vendor APIs (Greencom, IOThink, Lacroix,
// Schneider, WIT) and published to the asset.consumptions Kafka topic.
// ---------------------------------------------------------------------------

/** Type of asset: consumption, production, or both (battery). */
enum AssetType(val code: String):
  case Consumption           extends AssetType("CO")
  case Production            extends AssetType("PR")
  case ConsumptionProduction extends AssetType("CO-PR")

object AssetType:
  def fromCode(code: String): Option[AssetType] = values.find(_.code == code)
  given Encoder[AssetType] = Encoder.encodeString.contramap(_.code)

  given Decoder[AssetType] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown AssetType code: $s")
  )

/** Supported vendor connector types. */
enum AssetConnectorType(val code: String):
  case Greencom  extends AssetConnectorType("GREENCOM")
  case IOThink   extends AssetConnectorType("IOTHINK")
  case Lacroix   extends AssetConnectorType("LACROIX")
  case Schneider extends AssetConnectorType("SCHNEIDER")
  case Wit       extends AssetConnectorType("WIT")

object AssetConnectorType:
  def fromCode(code: String): Option[AssetConnectorType] = values.find(_.code == code)
  given Encoder[AssetConnectorType] = Encoder.encodeString.contramap(_.code)

  given Decoder[AssetConnectorType] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown AssetConnectorType code: $s")
  )

/**
 * Vendor-specific connection configuration.
 * Stored in the `{tenantId}.assetconnections` MongoDB collection.
 * Credentials are stored in plain text in this implementation
 * (production deployments should encrypt them).
 */
case class AssetConnection(
    id: String,
    name: String,
    connectorType: AssetConnectorType,
    url: String,
    refreshIntervalMins: Int,
    // Greencom / WIT: OAuth2 client credentials
    clientId: Option[String],
    clientSecret: Option[String],
    // WIT: separate authentication URL
    authenticationUrl: Option[String],
    // Schneider / IOThink / Lacroix: user + password
    user: Option[String],
    password: Option[String]
)

object AssetConnection:
  given Encoder[AssetConnection] = deriveEncoder
  given Decoder[AssetConnection] = deriveDecoder

/** One consumption measurement returned by a vendor connector. */
case class AssetMeasurement(
    timestamp: Instant,
    instantWatts: Double,
    instantWattsL1: Option[Double],
    instantWattsL2: Option[Double],
    instantWattsL3: Option[Double],
    instantAmps: Option[Double],
    instantAmpsL1: Option[Double],
    instantAmpsL2: Option[Double],
    instantAmpsL3: Option[Double],
    instantVolts: Option[Double],
    instantVoltsL1: Option[Double],
    instantVoltsL2: Option[Double],
    instantVoltsL3: Option[Double],
    consumptionWh: Double,        // energy in this interval (Wh)
    stateOfCharge: Option[Double] // battery SoC percentage (0–100)
)

object AssetMeasurement:
  given Encoder[AssetMeasurement] = deriveEncoder
  given Decoder[AssetMeasurement] = deriveDecoder

/**
 * Current consumption state persisted on the Asset document.
 * Updated after each successful poll.
 */
case class AssetCurrentConsumption(
    currentInstantWatts: Double,
    currentInstantWattsL1: Option[Double],
    currentInstantWattsL2: Option[Double],
    currentInstantWattsL3: Option[Double],
    currentInstantAmps: Option[Double],
    currentInstantAmpsL1: Option[Double],
    currentInstantAmpsL2: Option[Double],
    currentInstantAmpsL3: Option[Double],
    currentInstantVolts: Option[Double],
    currentInstantVoltsL1: Option[Double],
    currentInstantVoltsL2: Option[Double],
    currentInstantVoltsL3: Option[Double],
    currentConsumptionWh: Option[Double],
    currentTotalConsumptionWh: Double,
    currentStateOfCharge: Option[Double],
    lastConsumptionTimestamp: Option[Instant]
)

object AssetCurrentConsumption:

  val zero: AssetCurrentConsumption = AssetCurrentConsumption(
    currentInstantWatts = 0.0,
    currentInstantWattsL1 = None,
    currentInstantWattsL2 = None,
    currentInstantWattsL3 = None,
    currentInstantAmps = None,
    currentInstantAmpsL1 = None,
    currentInstantAmpsL2 = None,
    currentInstantAmpsL3 = None,
    currentInstantVolts = None,
    currentInstantVoltsL1 = None,
    currentInstantVoltsL2 = None,
    currentInstantVoltsL3 = None,
    currentConsumptionWh = None,
    currentTotalConsumptionWh = 0.0,
    currentStateOfCharge = None,
    lastConsumptionTimestamp = None
  )
  given Encoder[AssetCurrentConsumption] = deriveEncoder
  given Decoder[AssetCurrentConsumption] = deriveDecoder

/** Asset entity stored in `{tenantId}.assets`. */
case class Asset(
    id: String,
    name: String,
    companyId: String,
    siteId: Option[String],
    siteAreaId: String,
    assetType: AssetType,
    fluctuationPercent: Double,
    staticValueWatt: Double,
    coordinates: List[Double], // [longitude, latitude]
    issuer: Boolean,
    image: Option[String],
    dynamicAsset: Boolean,
    usesPushApi: Boolean,
    connectionId: Option[String],
    meterId: Option[String],
    excludeFromSmartCharging: Boolean,
    variationThresholdPercent: Option[Double],
    powerWattsLastSmartCharging: Option[Double],
    currentConsumption: AssetCurrentConsumption,
    createdOn: Instant,
    lastChangedOn: Instant
)

object Asset:
  given Encoder[Asset] = deriveEncoder
  given Decoder[Asset] = deriveDecoder

/** Event published to Kafka topic `asset.consumptions`. */
case class AssetConsumptionEvent(
    tenantId: String,
    assetId: String,
    assetName: String,
    siteAreaId: String,
    assetType: AssetType,
    measurement: AssetMeasurement
)

object AssetConsumptionEvent:
  given Encoder[AssetConsumptionEvent] = deriveEncoder
