package io.itbeans.ev.smartcharging

import io.circe.syntax._
import zio._

// ---------------------------------------------------------------------------
// SmartChargingGrpcServer — pre-codegen ADTs + handler for ev-smart-charging.
//
// Declared RPC methods (matching smart_charging_service.proto):
//   TriggerSmartCharging  — compute + apply profiles, returns counts
//   CheckConnection       — probe the SAP optimizer endpoint
//   BuildChargingProfiles — dry-run compute, no delivery
//
// The transport layer (SmartChargingGrpcTransport) bridges these to the
// ScalaPB-generated Future-based gRPC service interface.
// ---------------------------------------------------------------------------

// ── Request / Response ADTs (mirrors generated proto messages until codegen) ──

case class TriggerSmartChargingRequestADT(
    tenantId: String,
    siteAreaId: String,
    retry: Boolean = false
)

case class TriggerSmartChargingResponseADT(
    success: Boolean,
    profilesApplied: Int,
    profilesFailed: Int,
    error: String = ""
)

case class CheckConnectionRequestADT(tenantId: String)
case class CheckConnectionResponseADT(connected: Boolean, error: String = "")

case class BuildChargingProfilesRequestADT(
    tenantId: String,
    siteAreaId: String,
    excludedStationIds: List[String] = Nil
)

case class ChargingProfileResultADT(
    chargingStationId: String,
    connectorId: Int,
    chargingProfileJson: String,
    limitWatts: Double
)

case class BuildChargingProfilesResponseADT(
    profiles: List[ChargingProfileResultADT],
    error: String = ""
)

// ── Service handler trait ─────────────────────────────────────────────────

trait SmartChargingGrpcHandler:
  def triggerSmartCharging(req: TriggerSmartChargingRequestADT): Task[TriggerSmartChargingResponseADT]
  def checkConnection(req: CheckConnectionRequestADT): Task[CheckConnectionResponseADT]
  def buildChargingProfiles(req: BuildChargingProfilesRequestADT): Task[BuildChargingProfilesResponseADT]

// ── Implementation ────────────────────────────────────────────────────────

final class LiveSmartChargingGrpcHandler(
    service: SmartChargingService,
    optimizer: SapSmartChargingClient
) extends SmartChargingGrpcHandler:

  def triggerSmartCharging(req: TriggerSmartChargingRequestADT): Task[TriggerSmartChargingResponseADT] =
    service
      .triggerOptimization(req.tenantId, req.siteAreaId, "Reoptimize")
      .map { case (applied, failed) =>
        TriggerSmartChargingResponseADT(success = true, profilesApplied = applied, profilesFailed = failed)
      }
      .catchAll { err =>
        ZIO.logError(
          s"[SmartCharging] gRPC trigger failed for ${req.siteAreaId}: ${err.getMessage}"
        ) *>
          ZIO.succeed(TriggerSmartChargingResponseADT(
            success = false,
            profilesApplied = 0,
            profilesFailed = 0,
            error = err.getMessage
          ))
      }

  def checkConnection(req: CheckConnectionRequestADT): Task[CheckConnectionResponseADT] =
    optimizer.checkConnection
      .map(connected => CheckConnectionResponseADT(connected = connected))
      .catchAll(err => ZIO.succeed(CheckConnectionResponseADT(connected = false, error = err.getMessage)))

  def buildChargingProfiles(req: BuildChargingProfilesRequestADT): Task[BuildChargingProfilesResponseADT] =
    service
      .buildChargingProfiles(req.tenantId, req.siteAreaId, req.excludedStationIds)
      .map { profiles =>
        BuildChargingProfilesResponseADT(profiles = profiles.map(profileToResult))
      }
      .catchAll { err =>
        ZIO.logError(
          s"[SmartCharging] gRPC buildProfiles failed for ${req.siteAreaId}: ${err.getMessage}"
        ) *>
          ZIO.succeed(BuildChargingProfilesResponseADT(profiles = Nil, error = err.getMessage))
      }

  private def profileToResult(p: ChargingProfile): ChargingProfileResultADT =
    val peakWatts = p.profile.chargingSchedule.periods.map(_.limitWatts).maxOption.getOrElse(0.0)
    ChargingProfileResultADT(
      chargingStationId = p.chargingStationId,
      connectorId = p.connectorId,
      chargingProfileJson = p.profile.asJson.noSpaces,
      limitWatts = peakWatts
    )

object LiveSmartChargingGrpcHandler:

  val live: ZLayer[SmartChargingService & SapSmartChargingClient, Nothing, SmartChargingGrpcHandler] =
    ZLayer.fromFunction(new LiveSmartChargingGrpcHandler(_, _))
