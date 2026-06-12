package io.itbeans.ev.roaming

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

import java.time.Instant

// Circe instances for Instant.
private[roaming] object CirceInstances:
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    scala.util.Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  }

import CirceInstances.given

// ---------------------------------------------------------------------------
// RoamingDomain — domain types for ev-roaming (OCPI 2.1.1 + OICP 2.3.0).
//
// Protocol layer types use snake_case field names to match the OCPI 2.1.1 and
// OICP 2.3.0 wire formats exactly, avoiding mapping layers.
// Internal storage types use camelCase per Scala convention.
// ---------------------------------------------------------------------------

// ── OCPI 2.1.1 ───────────────────────────────────────────────────────────

enum OcpiRole(val code: String):
  case CPO  extends OcpiRole("CPO")  // Charge Point Operator
  case EMSP extends OcpiRole("EMSP") // eMobility Service Provider

object OcpiRole:
  def fromCode(s: String): Option[OcpiRole] = values.find(_.code == s)
  given Encoder[OcpiRole] = Encoder.encodeString.contramap(_.code)

  given Decoder[OcpiRole] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown OcpiRole: $s")
  )

enum OcpiEndpointStatus(val code: String):
  case Registered   extends OcpiEndpointStatus("REGISTERED")
  case Unregistered extends OcpiEndpointStatus("UNREGISTERED")
  case Pending      extends OcpiEndpointStatus("PENDING")

object OcpiEndpointStatus:
  def fromCode(s: String): Option[OcpiEndpointStatus] = values.find(_.code == s)
  given Encoder[OcpiEndpointStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[OcpiEndpointStatus] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown OcpiEndpointStatus: $s")
  )

/**
 * Registered OCPI roaming partner.
 * Stored in `{tenantId}.ocpiendpoints`.
 */
case class OcpiEndpoint(
    id: String,
    tenantId: String,
    countryCode: String, // partner country code (ISO 3166-1 alpha-2)
    partyId: String,     // partner party ID (3-char)
    role: OcpiRole,
    name: String,
    baseUrl: String,
    token: String,      // their token (we include in outbound requests)
    localToken: String, // our token (they include in inbound requests)
    status: OcpiEndpointStatus,
    backgroundPatchJob: Boolean,
    // Module endpoint URLs (populated after credential exchange)
    locationsUrl: Option[String],
    sessionsUrl: Option[String],
    cdrsUrl: Option[String],
    tariffUrl: Option[String],
    tokensUrl: Option[String],
    commandsUrl: Option[String],
    lastPatchJobOn: Option[Instant],
    lastPatchJobResult: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

object OcpiEndpoint:
  given Encoder[OcpiEndpoint] = deriveEncoder
  given Decoder[OcpiEndpoint] = deriveDecoder

// ── OCPI CDR (wire format — snake_case matches OCPI 2.1.1 spec) ──────────

case class OcpiGeoLocation(latitude: String, longitude: String)

object OcpiGeoLocation:
  given Encoder[OcpiGeoLocation] = deriveEncoder
  given Decoder[OcpiGeoLocation] = deriveDecoder

case class OcpiCdrLocation(
    id: String,
    name: Option[String],
    address: String,
    city: String,
    postal_code: String,
    country: String,
    coordinates: OcpiGeoLocation,
    evse_uid: String,
    evse_id: String,
    connector_id: String,
    connector_standard: String,  // IEC_62196_T2, CHADEMO, etc.
    connector_format: String,    // SOCKET, CABLE
    connector_power_type: String // AC_1_PHASE, AC_3_PHASE, DC
)

object OcpiCdrLocation:
  given Encoder[OcpiCdrLocation] = deriveEncoder
  given Decoder[OcpiCdrLocation] = deriveDecoder

case class OcpiCdrDimension(`type`: String, volume: Double)

object OcpiCdrDimension:
  given Encoder[OcpiCdrDimension] = deriveEncoder
  given Decoder[OcpiCdrDimension] = deriveDecoder

case class OcpiChargingPeriod(
    start_date_time: Instant,
    dimensions: List[OcpiCdrDimension],
    tariff_id: Option[String]
)

object OcpiChargingPeriod:
  given Encoder[OcpiChargingPeriod] = deriveEncoder
  given Decoder[OcpiChargingPeriod] = deriveDecoder

/**
 * OCPI 2.1.1 Charge Detail Record.
 * Field names match the OCPI spec exactly for JSON serialisation.
 */
case class OcpiCdr(
    id: String,
    start_date_time: Instant,
    stop_date_time: Instant,
    auth_id: String,     // idTag / badge ID
    auth_method: String, // AUTH_REQUEST | WHITELIST
    location: OcpiCdrLocation,
    currency: String,      // ISO 4217
    tariffs: List[String], // tariff IDs (simplified)
    charging_periods: List[OcpiChargingPeriod],
    total_cost: Double,
    total_energy: Double, // kWh
    total_time: Double,   // hours
    total_parking_time: Option[Double],
    remark: Option[String],
    last_updated: Instant
)

object OcpiCdr:
  given Encoder[OcpiCdr] = deriveEncoder
  given Decoder[OcpiCdr] = deriveDecoder

// ── OCPI generic response wrapper ─────────────────────────────────────────

case class OcpiResponse[A](
    status_code: Int,
    status_message: Option[String],
    timestamp: Instant,
    data: Option[A]
)

object OcpiResponse:

  def success[A](data: A): OcpiResponse[A] =
    OcpiResponse(1000, Some("Success"), Instant.now(), Some(data))

  def error(code: Int, msg: String): OcpiResponse[Nothing] =
    OcpiResponse(code, Some(msg), Instant.now(), None)
  given [A: Encoder]: Encoder[OcpiResponse[A]] = deriveEncoder
  given [A: Decoder]: Decoder[OcpiResponse[A]] = deriveDecoder

// ── OICP 2.3.0 ───────────────────────────────────────────────────────────

enum OicpEndpointStatus(val code: String):
  case Registered   extends OicpEndpointStatus("REGISTERED")
  case Unregistered extends OicpEndpointStatus("UNREGISTERED")

object OicpEndpointStatus:
  def fromCode(s: String): Option[OicpEndpointStatus] = values.find(_.code == s)
  given Encoder[OicpEndpointStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[OicpEndpointStatus] = Decoder.decodeString.emap(s =>
    fromCode(s).toRight(s"Unknown OicpEndpointStatus: $s")
  )

/**
 * Registered OICP roaming partner (typically Hubject).
 * Stored in `{tenantId}.oicpendpoints`.
 */
case class OicpEndpoint(
    id: String,
    tenantId: String,
    name: String,
    baseUrl: String,
    countryCode: String,
    partyId: String,
    status: OicpEndpointStatus,
    operatorId: String, // OICP Operator ID (e.g. "+49*XYZ")
    operatorName: String,
    lastPatchJobOn: Option[Instant],
    lastPatchJobResult: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

object OicpEndpoint:
  given Encoder[OicpEndpoint] = deriveEncoder
  given Decoder[OicpEndpoint] = deriveDecoder

/** OICP identification (RFID/eMAID/RemoteAuth). */
case class OicpIdentification(
    RFIDMifareFamilyIdentification: Option[OicpRfidId],
    RemoteIdentification: Option[OicpRemoteId]
)

object OicpIdentification:
  given Encoder[OicpIdentification] = deriveEncoder
  given Decoder[OicpIdentification] = deriveDecoder

case class OicpRfidId(UID: String)

object OicpRfidId:
  given Encoder[OicpRfidId] = deriveEncoder
  given Decoder[OicpRfidId] = deriveDecoder

case class OicpRemoteId(EvcoID: String)

object OicpRemoteId:
  given Encoder[OicpRemoteId] = deriveEncoder
  given Decoder[OicpRemoteId] = deriveDecoder

/**
 * OICP 2.3.0 Charge Detail Record.
 * Field names match the OICP spec (PascalCase).
 */
case class OicpChargeDetailRecord(
    SessionID: String,
    CPOPartnerSessionID: Option[String],
    EMPPartnerSessionID: Option[String],
    EvseID: String,
    Identification: OicpIdentification,
    SessionStart: Instant,
    SessionEnd: Instant,
    MeterValueStart: Double,
    MeterValueEnd: Double,
    ConsumedEnergy: Double, // kWh
    ChargingStart: Instant,
    ChargingEnd: Instant,
    HubOperatorID: Option[String],
    HubProviderID: Option[String]
)

object OicpChargeDetailRecord:
  given Encoder[OicpChargeDetailRecord] = deriveEncoder
  given Decoder[OicpChargeDetailRecord] = deriveDecoder

// ── Connector type mapping: OCPP ConnectorType → OCPI standard ───────────

/**
 * Maps the OCPP/domain connector type to OCPI 2.1.1 standard,
 * format, and power type strings.
 */
object OcpiConnectorMapping:

  def toOcpi(connectorType: Option[String]): (String, String, String) =
    connectorType.map(_.toUpperCase).getOrElse("") match
      case t if t.contains("CCS") || t.contains("COMBO") =>
        ("IEC_62196_T2_COMBO", "CABLE", "DC")
      case t if t.contains("CHADEMO") || t.contains("CHAdeMO") =>
        ("CHADEMO", "CABLE", "DC")
      case t if t.contains("TYPE1") || t.contains("J1772") =>
        ("IEC_62196_T1", "SOCKET", "AC_1_PHASE")
      case t if t.contains("TYPE2") || t.contains("MENNEKES") || t.isEmpty =>
        ("IEC_62196_T2", "SOCKET", "AC_3_PHASE")
      case t if t.contains("GBT") || t.contains("GB_T") =>
        ("GB_T_20234_2", "SOCKET", "AC_3_PHASE")
      case t if t.contains("TESLA") =>
        ("TESLA_R", "CABLE", "DC")
      case _ =>
        ("IEC_62196_T2", "SOCKET", "AC_3_PHASE")

// ── Charging station location (read model for CDR enrichment) ─────────────

/**
 * Minimal location data fetched from the charging station + site documents
 * and embedded into OCPI CDRs.
 */
case class StationLocation(
    stationId: String,
    connectorType: Option[String],
    address: String,
    city: String,
    postalCode: String,
    country: String,
    latitude: String,
    longitude: String,
    siteName: Option[String]
)

// ── Internal transaction read model ──────────────────────────────────────

/**
 * Minimal transaction representation used by CdrService.
 * Read from `{tenantId}.transactions` at CDR generation time.
 */
case class RoamingTransaction(
    id: Long,
    chargingStationId: String,
    connectorId: Int,
    evseId: Option[Int],
    tagId: Option[String],
    userId: Option[String],
    currency: Option[String],
    startDate: Instant,
    endDate: Instant,
    meterStart: Double,
    meterStop: Double,
    totalConsumptionWh: Double,
    totalDurationSecs: Long,
    totalInactivitySecs: Long,
    totalCost: Option[Double],
    siteId: Option[String],
    // Roaming metadata (from OCPI/OICP session if applicable)
    ocpiSessionId: Option[String],
    ocpiEvseId: Option[String]
)

// ── Kafka payload for transactions.lifecycle ──────────────────────────────

/**
 * Subset of TransactionLifecycleEvent that the roaming service decodes.
 * Published by ev-ocpp-processor on transaction start/update/stop.
 */
case class TransactionLifecyclePayload(
    tenantId: String,
    transactionId: Long,
    action: String, // "Start" | "Update" | "Stop"
    chargingStationId: String,
    connectorId: Int,
    meterValue: Double,
    stopReason: Option[String]
)

object TransactionLifecyclePayload:
  // ev-ocpp-processor emits transactionId as a JSON number for OCPP 1.6 and as
  // a string for OCPP 2.x station-generated ids — accept both when numeric.
  private given Decoder[Long] =
    Decoder.decodeLong.or(
      Decoder.decodeString.emap(s => s.toLongOption.toRight(s"non-numeric Long: $s"))
    )
  given Decoder[TransactionLifecyclePayload] = deriveDecoder
