package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}

opaque type ChargingStationId = String

object ChargingStationId:
  def apply(v: String): ChargingStationId             = v
  def unapply(id: ChargingStationId): Some[String]    = Some(id)
  extension (id: ChargingStationId) def value: String = id

  given Schema[ChargingStationId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(ChargingStationId(_), _.value)

/** The OCPP version the station has negotiated with the gateway. */
enum OcppVersion:
  case V1_6, V2_0_1, V2_1

/** Which transport the station is using. */
enum OcppTransport:
  case Json // WebSocket (OCPP-J)
  case Soap // SOAP (OCPP-S, legacy 1.2/1.5 only)

enum ConnectorStatus:
  case Available, Occupied, Reserved, Unavailable, Faulted

enum ConnectorType:

  // AC
  case Type1, Type2, CCS1, CCS2, CHAdeMO, Schuko, CEE3, CEE5,
    // DC bidirectional (OCPP 2.1 / ISO 15118-20)
    CCS2Bidirectional, CHAdeMOBidirectional,
    // Battery swap
    BatterySwap,
    Unknown

case class Connector(
    connectorId: Int,
    status: ConnectorStatus,
    connectorType: ConnectorType,
    amperage: Option[Int],
    voltage: Option[Int],
    wattage: Option[Double],
    // OCPP 2.1: bidirectional power support
    bidirectional: Boolean = false,
    // OCPP 2.1: ISO 15118-20 plug-and-charge capable
    iso15118: Boolean = false,
    currentType: CurrentType = CurrentType.AC
)

enum CurrentType:
  case AC, DC, ACAndDC

case class ChargingStation(
    id: ChargingStationId,
    tenantId: TenantId,
    siteId: Option[SiteId],
    siteAreaId: Option[SiteAreaId],
    companyId: Option[CompanyId],
    chargePointVendor: String,
    chargePointModel: String,
    chargePointSerialNumber: Option[String],
    firmwareVersion: Option[String],
    ocppVersion: Option[OcppVersion],
    ocppTransport: OcppTransport,
    ocppProtocol: Option[String],
    connectors: List[Connector],
    coordinates: Option[Coordinates],
    address: Option[Address],
    inactive: Boolean,
    forceInactive: Boolean,
    manualConfiguration: Boolean,
    maximumPower: Option[Double],
    excludeFromSmartCharging: Boolean,
    public: Boolean,
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant,
    lastHeartbeat: Option[java.time.Instant],
    // OCPP 2.1: Device Model components/variables (replaces OCPP 1.x key/value config)
    deviceModel: Map[String, Map[String, String]],
    // OCPP 2.1: V2X capability flags
    v2xCapable: Boolean = false,
    derCapable: Boolean = false,
    batterySwapCapable: Boolean = false
)

object ChargingStation:
  given Schema[OcppVersion] = DeriveSchema.gen
  given Schema[OcppTransport] = DeriveSchema.gen
  given Schema[ConnectorStatus] = DeriveSchema.gen
  given Schema[ConnectorType] = DeriveSchema.gen
  given Schema[CurrentType] = DeriveSchema.gen
  given Schema[Connector] = DeriveSchema.gen
  given Schema[ChargingStation] = DeriveSchema.gen
