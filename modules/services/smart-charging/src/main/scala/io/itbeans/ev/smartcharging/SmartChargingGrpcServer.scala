package io.itbeans.ev.smartcharging

import zio._

// ---------------------------------------------------------------------------
// SmartChargingGrpcServer — pre-codegen gRPC server for ev-smart-charging.
//
// In production this will be generated from smart_charging_service.proto via
// ScalaPB + zio-grpc. Until then, these ADTs define the contract and the
// business logic is callable in-process.
//
// Declared RPC methods (matching smart_charging_service.proto):
//   TriggerSmartCharging(tenantId, siteAreaId, event) → TriggerResponse
//   GetChargingProfiles(tenantId, siteAreaId) → ChargingProfilesResponse
//   DeleteChargingProfiles(tenantId, chargingStationId) → DeleteResponse
// ---------------------------------------------------------------------------

// ── Request / Response ADTs (will be replaced by ScalaPB generated classes) ──

case class TriggerSmartChargingRequestADT(
    tenantId: String,
    siteAreaId: String,
    event: String // "Reoptimize" | "CarArrival" | "CarDeparture"
)
case class TriggerSmartChargingResponseADT(success: Boolean, message: String)

case class GetChargingProfilesRequestADT(tenantId: String, siteAreaId: String)

case class ChargingProfileSummaryADT(
    id: String,
    chargingStationId: String,
    connectorId: Int,
    stackLevel: Int,
    purpose: String
)
case class GetChargingProfilesResponseADT(profiles: List[ChargingProfileSummaryADT])

case class DeleteChargingProfilesRequestADT(tenantId: String, chargingStationId: String)
case class DeleteChargingProfilesResponseADT(deleted: Int)

// ── Service implementation ────────────────────────────────────────────────

trait SmartChargingGrpcHandler:
  def triggerSmartCharging(req: TriggerSmartChargingRequestADT): Task[TriggerSmartChargingResponseADT]
  def getChargingProfiles(req: GetChargingProfilesRequestADT): Task[GetChargingProfilesResponseADT]
  def deleteChargingProfiles(req: DeleteChargingProfilesRequestADT): Task[DeleteChargingProfilesResponseADT]

final class LiveSmartChargingGrpcHandler(
    service: SmartChargingService,
    repo: SmartChargingRepository
) extends SmartChargingGrpcHandler:

  def triggerSmartCharging(req: TriggerSmartChargingRequestADT): Task[TriggerSmartChargingResponseADT] =
    service
      .triggerOptimization(req.tenantId, req.siteAreaId, req.event)
      .as(TriggerSmartChargingResponseADT(success = true, message = "Optimization triggered"))
      .catchAll { err =>
        ZIO.logError(
          s"[SmartCharging] gRPC trigger failed for ${req.siteAreaId}: ${err.getMessage}"
        ) *>
          ZIO.succeed(TriggerSmartChargingResponseADT(success = false, message = err.getMessage))
      }

  def getChargingProfiles(req: GetChargingProfilesRequestADT): Task[GetChargingProfilesResponseADT] =
    repo.findProfiles(req.tenantId, req.siteAreaId)
      .map { profiles =>
        GetChargingProfilesResponseADT(
          profiles.map(p =>
            ChargingProfileSummaryADT(
              id = p.id,
              chargingStationId = p.chargingStationId,
              connectorId = p.connectorId,
              stackLevel = p.profile.stackLevel,
              purpose = p.profile.chargingProfilePurpose
            )
          )
        )
      }

  def deleteChargingProfiles(req: DeleteChargingProfilesRequestADT): Task[DeleteChargingProfilesResponseADT] =
    repo.deleteProfilesForStation(req.tenantId, req.chargingStationId)
      .as(DeleteChargingProfilesResponseADT(deleted = 1)) // MongoDB doesn't return count on deleteMany in this wrapper

object LiveSmartChargingGrpcHandler:

  val live: ZLayer[SmartChargingService & SmartChargingRepository, Nothing, SmartChargingGrpcHandler] =
    ZLayer.fromFunction(new LiveSmartChargingGrpcHandler(_, _))
