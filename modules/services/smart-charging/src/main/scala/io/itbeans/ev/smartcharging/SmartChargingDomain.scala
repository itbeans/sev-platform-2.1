package io.itbeans.ev.smartcharging

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._

import java.time.Instant

// ---------------------------------------------------------------------------
// ev-smart-charging domain types
//
// ChargingProfile: OCPP 1.6/2.0.1/2.1 compatible structure stored in MongoDB.
// SAP Optimizer: request/response types for the external SAP API.
// ---------------------------------------------------------------------------

// ── OCPP Charging Profile ─────────────────────────────────────────────────

/**
 * One period within a charging schedule.
 * startPeriod: seconds from scheduleStart.
 * limitWatts:  max allowed power for this period (W).
 */
case class ChargingSchedulePeriod(
    startPeriod: Int,
    limitWatts: Double,
    numberPhases: Option[Int] = None
)

object ChargingSchedulePeriod:
  given Encoder[ChargingSchedulePeriod] = deriveEncoder
  given Decoder[ChargingSchedulePeriod] = deriveDecoder

case class ChargingSchedule(
    // Aligned to last 15-minute mark; None = relative to transaction start
    startSchedule: Option[Instant],
    durationSecs: Option[Int],
    chargingRateUnit: String, // "W" (watts) — we normalise from A at ingestion
    periods: List[ChargingSchedulePeriod],
    minChargingRate: Option[Double]
)

object ChargingSchedule:
  given Encoder[ChargingSchedule] = deriveEncoder
  given Decoder[ChargingSchedule] = deriveDecoder

case class ChargingProfileData(
    chargingProfileId: Int,
    transactionId: Option[Long],
    stackLevel: Int,                // 0 = station-wide; 2 = TX_PROFILE
    chargingProfilePurpose: String, // "TxDefaultProfile" | "TxProfile" | "ChargePointMaxProfile"
    chargingProfileKind: String,    // "Absolute" | "Recurring" | "Relative"
    recurrencyKind: Option[String],
    validFrom: Option[Instant],
    validTo: Option[Instant],
    chargingSchedule: ChargingSchedule
)

object ChargingProfileData:
  given Encoder[ChargingProfileData] = deriveEncoder
  given Decoder[ChargingProfileData] = deriveDecoder

case class ChargingProfile(
    id: String,
    tenantId: String,
    chargingStationId: String,
    connectorId: Int,
    siteAreaId: Option[String],
    siteId: Option[String],
    profile: ChargingProfileData,
    createdOn: Instant,
    lastChangedOn: Instant
)

object ChargingProfile:
  given Encoder[ChargingProfile] = deriveEncoder
  given Decoder[ChargingProfile] = deriveDecoder

// ── SAP Smart Charging Optimizer types ────────────────────────────────────

/**
 * A car (EV session) as the SAP optimizer sees it.
 * `name` = "chargingStationId~connectorId"
 * `currentPlan` = list of up to 96 amperage values (15-min slots)
 */
case class OptimizerCar(
    id: Int,
    name: String,               // "{chargingStationId}~{connectorId}"
    carType: String,            // "BEV" | "PHEV"
    startCapacity: Double,      // current energy in battery (kWh)
    maxCapacity: Double,        // battery capacity (kWh)
    minCurrentPerPhase: Double, // minimum charging current per phase (A)
    maxCurrentPerPhase: Double, // maximum charging current per phase (A)
    canLoadPhase1: Int,         // 1 if phase connected, 0 otherwise
    canLoadPhase2: Int,
    canLoadPhase3: Int,
    currentPlan: List[Double] // amperage per 15-min slot (returned by optimizer)
)

object OptimizerCar:
  given Encoder[OptimizerCar] = deriveEncoder
  given Decoder[OptimizerCar] = deriveDecoder

/** A fuse node in the site area topology (site area → charging stations). */
case class OptimizerFuse(
    `@type`: String = "Fuse",
    id: Int,
    fusePhase1: Double, // max current on phase 1 (A)
    fusePhase2: Double,
    fusePhase3: Double,
    phase1Connected: Boolean = true,
    phase2Connected: Boolean = true,
    phase3Connected: Boolean = true,
    children: List[OptimizerFuse] = Nil
)

object OptimizerFuse:
  given Encoder[OptimizerFuse] = deriveEncoder
  given Decoder[OptimizerFuse] = deriveDecoder

case class CarAssignment(carID: Int, chargingStationID: Int)

object CarAssignment:
  given Encoder[CarAssignment] = deriveEncoder
  given Decoder[CarAssignment] = deriveDecoder

case class OptimizerState(
    fuseTree: Map[String, OptimizerFuse],
    cars: List[OptimizerCar],
    carAssignments: List[CarAssignment],
    currentTimeSeconds: Int // seconds into current 15-min slot
)

object OptimizerState:
  given Encoder[OptimizerState] = deriveEncoder
  given Decoder[OptimizerState] = deriveDecoder

case class OptimizerEvent(eventType: String) // "Reoptimize" | "CarArrival" | ...

object OptimizerEvent:
  given Encoder[OptimizerEvent] = deriveEncoder
  given Decoder[OptimizerEvent] = deriveDecoder

case class OptimizerRequest(event: OptimizerEvent, state: OptimizerState)

object OptimizerRequest:
  given Encoder[OptimizerRequest] = deriveEncoder
  given Decoder[OptimizerRequest] = deriveDecoder

case class OptimizerResult(cars: List[OptimizerCar])

object OptimizerResult:
  given Encoder[OptimizerResult] = deriveEncoder
  given Decoder[OptimizerResult] = deriveDecoder

// ── Kafka payload types ────────────────────────────────────────────────────

/** Published to `smart-charging.triggers` by ev-ocpp-processor and ev-rest-api. */
case class SmartChargingTriggerPayload(
    tenantId: String,
    siteAreaId: String,
    event: String, // "Reoptimize" | "CarArrival" | "CarDeparture"
    chargingStationId: Option[String],
    connectorId: Option[Int]
)

object SmartChargingTriggerPayload:
  given Encoder[SmartChargingTriggerPayload] = deriveEncoder
  given Decoder[SmartChargingTriggerPayload] = deriveDecoder

/** Snapshot published to `asset.consumptions` by ev-asset. */
case class AssetConsumptionPayload(
    tenantId: String,
    siteAreaId: String,
    powerWatts: Double,
    assetType: String // "Production" | "Consumption" | "ConsumptionProduction"
)

object AssetConsumptionPayload:
  given Encoder[AssetConsumptionPayload] = deriveEncoder
  given Decoder[AssetConsumptionPayload] = deriveDecoder

// ── Lightweight site-area read model (subset needed by smart charging) ────

case class SiteAreaSC(
    id: String,
    tenantId: String,
    siteId: Option[String],
    name: String,
    maximumPower: Double,        // W — grid fuse
    voltage: Option[Int],        // V (default 230)
    numberOfPhases: Option[Int], // default 3
    smartCharging: Boolean
)

object SiteAreaSC:
  given Encoder[SiteAreaSC] = deriveEncoder
  given Decoder[SiteAreaSC] = deriveDecoder

/** Active transaction as seen by the optimizer (subset of full transaction). */
case class ActiveTransactionSC(
    id: Long,
    chargingStationId: String,
    connectorId: Int,
    stateOfCharge: Option[Int],     // % (if available from MeterValues)
    departureTime: Option[Instant], // from smart-charging session params
    targetSoC: Option[Int],         // % target
    maxCurrentA: Double,            // from connector capability
    numberOfPhases: Int
)

object ActiveTransactionSC:
  given Encoder[ActiveTransactionSC] = deriveEncoder
  given Decoder[ActiveTransactionSC] = deriveDecoder
