package io.itbeans.ev.ocppprocessor

import io.itbeans.ev.domain.{ChargingStationId, ConnectorStatus, OcppVersion, TenantId, TransactionId}

import java.time.Instant

// ---------------------------------------------------------------------------
// OcppEvents — domain event types consumed from Kafka by ev-ocpp-processor.
//
// These are deserialized from the JSON payloads published by ev-ocpp-gateway.
// Each case class maps to one OCPP action.
//
// Design: all fields are plain primitives / Strings to simplify JSON round-trip.
// Type-safe domain IDs are applied inside the handler methods.
// ---------------------------------------------------------------------------

/** Raw event as consumed from Kafka (before dispatch). */
case class RawOcppEvent(
    tenantId: String,
    stationId: String,
    version: String,  // "V1_6" | "V2_0_1" | "V2_1"
    messageType: Int, // 2 = CALL, 6 = SEND
    uniqueId: String,
    action: String,
    payloadJson: String
)

// ── Individual event payloads ─────────────────────────────────────────────

/** OCPP BootNotification — station announces itself on (re)connect. */
case class BootNotificationEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    ocppVersion: OcppVersion,
    uniqueId: String,
    vendor: String,
    model: String,
    serialNumber: Option[String],
    firmwareVersion: Option[String],
    reason: String, // OCPP 2.1: "PowerUp" | "Reboot" | "Scheduled" | ...
    timestamp: Instant
)

/** OCPP StatusNotification — connector status change. */
case class StatusChangedEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    connectorId: Int,
    evseId: Option[Int], // OCPP 2.x
    status: ConnectorStatus,
    timestamp: Instant
)

/** OCPP transaction lifecycle — covers Start/Update/End for both 1.6 and 2.1. */
case class TransactionLifecycleEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    transactionId: String, // String for OCPP 2.x UUIDs; Long converted from 1.6
    action: TransactionAction,
    connectorId: Int,
    evseId: Option[Int],
    tagId: Option[String],
    timestamp: Instant,
    meterValueWh: Double,
    instantWatts: Option[Double],
    stateOfCharge: Option[Int],
    stopReason: Option[String]
)

enum TransactionAction:
  case Started, Updated, Ended

/** OCPP Authorize — validate an RFID badge / eMAID. */
case class AuthorizeEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    uniqueId: String,
    idTag: String,
    tokenType: String,
    timestamp: Instant
)

/** OCPP MeterValues — periodic power readings (1.6 only; 2.x uses TransactionEvent). */
case class MeterValuesEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    transactionId: Option[Long],
    connectorId: Int,
    timestamp: Instant,
    instantWatts: Double,
    cumulatedKwh: Double
)

/** OCPP 2.1 NotifyDERStartStop — DER (V2X/solar) power flow event. */
case class DERStartStopEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    started: Boolean,
    evseId: Int,
    timestamp: Instant
)

/** OCPP 2.1 BatterySwap — battery swap event for modular EVs. */
case class BatterySwapEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    eventType: String, // "BatteryIn" | "BatteryOut"
    evseId: Int,
    timestamp: Instant
)

/** OCPP 2.1 AFRRSignal — Automatic Frequency Restoration Reserve signal. */
case class AFRRSignalEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    activationTime: Instant,
    signal: Int, // in mW
    timestamp: Instant
)

/** Heartbeat event — station is alive. */
case class HeartbeatEvent(
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    timestamp: Instant
)
