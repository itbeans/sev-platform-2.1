package io.itbeans.ev.domain

import io.itbeans.ev.domain.ocpp.{
  EVSE,
  IdToken,
  MeterValue,
  OcppAction,
  TransactionEventType,
  TransactionInfo,
  TriggerReason
}
import zio.schema.{DeriveSchema, Schema}

// ---------------------------------------------------------------------------
// Internal domain events — published to Kafka by the OCPP Gateway.
// These are the canonical internal representation regardless of whether
// the originating station spoke OCPP 1.6 or OCPP 2.1.
// ---------------------------------------------------------------------------

sealed trait OcppEvent:
  def tenantId: TenantId
  def chargingStationId: ChargingStationId
  def timestamp: java.time.Instant
  def ocppVersion: OcppVersion

/** Station connected to the gateway. */
case class StationConnectedEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    remoteAddr: String
) extends OcppEvent

/** Station sent BootNotification. */
case class BootNotificationEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    vendor: String,
    model: String,
    firmwareVersion: Option[String]
) extends OcppEvent

/** Connector/EVSE status changed. */
case class StatusChangedEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    connectorId: Int,
    evseId: Option[Int],
    status: ConnectorStatus
) extends OcppEvent

/** Authorization request from the station. */
case class AuthorizeEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    idTag: String,
    tokenType: String,
    correlationId: String // OCPP call uniqueId — response routed back via this
) extends OcppEvent

/**
 * Transaction lifecycle (start / update / stop). Normalised from both
 *  OCPP 1.6 StartTransaction/StopTransaction and OCPP 2.1 TransactionEvent.
 */
case class TransactionLifecycleEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    transactionId: TransactionId,
    action: TransactionAction,
    connectorId: Int,
    evseId: Option[Int],
    idTag: Option[String],
    meterValue: Double,
    meterValues: List[MeterValue],
    stopReason: Option[String],
    correlationId: String
) extends OcppEvent

/** Meter values batch. */
case class MeterValuesEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    transactionId: Option[TransactionId],
    connectorId: Int,
    evseId: Option[Int],
    meterValues: List[MeterValue]
) extends OcppEvent

/** OCPP 2.1: DER start/stop notification. */
case class DERStartStopEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    started: Boolean,
    controlId: Option[Int]
) extends OcppEvent

/** OCPP 2.1: Battery swap notification. */
case class BatterySwapEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion,
    eventType: String,
    batteryId: Option[String],
    soC: Option[Int]
) extends OcppEvent

/** Station disconnected from the gateway. */
case class StationDisconnectedEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: java.time.Instant,
    ocppVersion: OcppVersion
) extends OcppEvent

// ---------------------------------------------------------------------------
// Commands sent from CSMS to station (via OCPP Gateway gRPC).
// These are queued in the Gateway and delivered over the live WebSocket.
// ---------------------------------------------------------------------------

sealed trait OcppCommand:
  def tenantId: TenantId
  def chargingStationId: ChargingStationId
  def correlationId: String

case class RemoteStartCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    connectorId: Option[Int],
    evseId: Option[Int],
    idTag: String,
    chargingProfile: Option[String] // JSON-encoded ChargingProfile
) extends OcppCommand

case class RemoteStopCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    transactionId: TransactionId
) extends OcppCommand

case class SetChargingProfileCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    connectorId: Int,
    profile: String // JSON-encoded ChargingProfile
) extends OcppCommand

case class ResetCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    resetType: ResetType,
    evseId: Option[Int]
) extends OcppCommand

enum ResetType:
  case Immediate, OnIdle

object Events:
  given Schema[ResetType] = DeriveSchema.gen
  given Schema[StationConnectedEvent] = DeriveSchema.gen
  given Schema[BootNotificationEvent] = DeriveSchema.gen
  given Schema[StatusChangedEvent] = DeriveSchema.gen
  given Schema[AuthorizeEvent] = DeriveSchema.gen
  given Schema[TransactionLifecycleEvent] = DeriveSchema.gen
  given Schema[MeterValuesEvent] = DeriveSchema.gen
  given Schema[DERStartStopEvent] = DeriveSchema.gen
  given Schema[BatterySwapEvent] = DeriveSchema.gen
  given Schema[StationDisconnectedEvent] = DeriveSchema.gen
  given Schema[OcppEvent] = DeriveSchema.gen
  given Schema[RemoteStartCommand] = DeriveSchema.gen
  given Schema[RemoteStopCommand] = DeriveSchema.gen
  given Schema[SetChargingProfileCommand] = DeriveSchema.gen
  given Schema[ResetCommand] = DeriveSchema.gen
  given Schema[SetDERControlCommand] = DeriveSchema.gen
  given Schema[AFRRSignalCommand] = DeriveSchema.gen
  given Schema[OcppCommand] = DeriveSchema.gen

/** OCPP 2.1: Send DER control settings to station. */
case class SetDERControlCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    controlData: String // JSON-encoded DERControlRequest
) extends OcppCommand

/** OCPP 2.1: Send AFRR signal to station. */
case class AFRRSignalCommand(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    correlationId: String,
    signal: Long,
    timestamp: java.time.Instant
) extends OcppCommand
