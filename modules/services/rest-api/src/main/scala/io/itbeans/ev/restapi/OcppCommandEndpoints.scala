package io.itbeans.ev.restapi

import io.circe.generic.auto._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// OcppCommandEndpoints — remote commands to charging stations.
//
// All commands are forwarded via gRPC to ev-ocpp-gateway, which holds the
// live WebSocket connections. The gateway relays the OCPP request and
// returns the station's response.
//
// Mirrors: ChargingStationService.ts — remote action endpoints.
// OCPP 1.6: Reset, ClearCache, RemoteStartTransaction, RemoteStopTransaction,
//            UnlockConnector, GetDiagnostics, UpdateFirmware, TriggerMessage,
//            GetConfiguration, ChangeConfiguration, GetCompositeSchedule,
//            SetChargingProfile, ClearChargingProfile, DataTransfer.
// OCPP 2.1: SetVariables, GetVariables, SetDERControl, GetDERControl,
//            SendAFRRSignal, RequestStartTransaction, RequestStopTransaction.
// ---------------------------------------------------------------------------

// Command request / response DTOs
case class ResetRequest(stationId: String, resetType: String) // "Hard" | "Soft"
case class RemoteStartRequest(stationId: String, connectorId: Option[Int], tagId: String, carId: Option[String])
case class RemoteStopRequest(stationId: String, transactionId: Long)
case class UnlockConnectorRequest(stationId: String, connectorId: Int)
case class TriggerMessageRequest(stationId: String, requestedMessage: String, connectorId: Option[Int])
case class ChangeConfigurationRequest(stationId: String, key: String, value: String)
case class DataTransferRequest(stationId: String, vendorId: String, messageId: Option[String], data: Option[String])
case class SetChargingProfileRequest(stationId: String, connectorId: Int, chargingProfile: io.circe.Json)

case class GetCompositeScheduleRequest(
    stationId: String,
    connectorId: Int,
    duration: Int,
    chargingRateUnit: Option[String]
)

case class ClearChargingProfileRequest(
    stationId: String,
    connectorId: Option[Int],
    id: Option[Int],
    chargingProfilePurpose: Option[String]
)

case class ChangeAvailabilityRequest(
    stationId: String,
    connectorId: Int,
    availabilityType: String
) // "Operative" | "Inoperative"

case class GetDiagnosticsRequest(
    stationId: String,
    location: String,
    retries: Option[Int],
    retryInterval: Option[Int],
    startTime: Option[Long],
    stopTime: Option[Long]
)

case class UpdateFirmwareRequest(
    stationId: String,
    location: String,
    retrieveDate: Long,
    retries: Option[Int],
    retryInterval: Option[Int]
)
case class ReserveNowRequest(stationId: String, connectorId: Int, expiryDate: Long, idTag: String, reservationId: Int)
case class CancelReservationRequest(stationId: String, reservationId: Int)
case class CommandResult(status: String, info: Option[String])

object OcppCommandEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)
  private val stationBase  = base.in("api" / "v1" / "ChargingStations" / path[String]("id"))

  // ── Reset ─────────────────────────────────────────────────────────────────
  private val resetEp = stationBase.post
    .in("Action" / "Reset")
    .in(tenantHeader)
    .in(jsonBody[ResetRequest])
    .out(jsonBody[CommandResult])

  // ── Clear Cache ───────────────────────────────────────────────────────────
  private val clearCacheEp = stationBase.post
    .in("Action" / "ClearCache")
    .in(tenantHeader)
    .out(jsonBody[CommandResult])

  // ── Remote Start ──────────────────────────────────────────────────────────
  private val remoteStartEp = stationBase.post
    .in("Action" / "RemoteStartTransaction")
    .in(tenantHeader)
    .in(jsonBody[RemoteStartRequest])
    .out(jsonBody[CommandResult])

  // ── Remote Stop ───────────────────────────────────────────────────────────
  private val remoteStopEp = stationBase.post
    .in("Action" / "RemoteStopTransaction")
    .in(tenantHeader)
    .in(jsonBody[RemoteStopRequest])
    .out(jsonBody[CommandResult])

  // ── Unlock Connector ──────────────────────────────────────────────────────
  private val unlockEp = stationBase.post
    .in("Action" / "UnlockConnector")
    .in(tenantHeader)
    .in(jsonBody[UnlockConnectorRequest])
    .out(jsonBody[CommandResult])

  // ── Trigger Message ───────────────────────────────────────────────────────
  private val triggerMessageEp = stationBase.post
    .in("Action" / "TriggerMessage")
    .in(tenantHeader)
    .in(jsonBody[TriggerMessageRequest])
    .out(jsonBody[CommandResult])

  // ── Change Configuration (OCPP 1.x) / SetVariables (OCPP 2.x) ────────────
  private val changeConfigEp = stationBase.post
    .in("Action" / "ChangeConfiguration")
    .in(tenantHeader)
    .in(jsonBody[ChangeConfigurationRequest])
    .out(jsonBody[CommandResult])

  // ── Get Configuration ─────────────────────────────────────────────────────
  private val getConfigEp = stationBase.get
    .in("Configuration")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  // ── Data Transfer ─────────────────────────────────────────────────────────
  private val dataTransferEp = stationBase.post
    .in("Action" / "DataTransfer")
    .in(tenantHeader)
    .in(jsonBody[DataTransferRequest])
    .out(jsonBody[CommandResult])

  // ── Set Charging Profile ──────────────────────────────────────────────────
  private val setChargingProfileEp = stationBase.post
    .in("Action" / "SetChargingProfile")
    .in(tenantHeader)
    .in(jsonBody[SetChargingProfileRequest])
    .out(jsonBody[CommandResult])

  // ── Get Composite Schedule ────────────────────────────────────────────────
  private val getCompositeScheduleEp = stationBase.post
    .in("Action" / "GetCompositeSchedule")
    .in(tenantHeader)
    .in(jsonBody[GetCompositeScheduleRequest])
    .out(jsonBody[io.circe.Json])

  // ── Clear Charging Profile ────────────────────────────────────────────────
  private val clearChargingProfileEp = stationBase.post
    .in("Action" / "ClearChargingProfile")
    .in(tenantHeader)
    .in(jsonBody[ClearChargingProfileRequest])
    .out(jsonBody[CommandResult])

  // ── Change Availability ───────────────────────────────────────────────────
  private val changeAvailabilityEp = stationBase.post
    .in("Action" / "ChangeAvailability")
    .in(tenantHeader)
    .in(jsonBody[ChangeAvailabilityRequest])
    .out(jsonBody[CommandResult])

  // ── Get Diagnostics ───────────────────────────────────────────────────────
  private val getDiagnosticsEp = stationBase.post
    .in("Action" / "GetDiagnostics")
    .in(tenantHeader)
    .in(jsonBody[GetDiagnosticsRequest])
    .out(jsonBody[CommandResult])

  // ── Update Firmware ───────────────────────────────────────────────────────
  private val updateFirmwareEp = stationBase.post
    .in("Action" / "UpdateFirmware")
    .in(tenantHeader)
    .in(jsonBody[UpdateFirmwareRequest])
    .out(statusCode(sttp.model.StatusCode.Accepted))

  // ── Reserve Now ───────────────────────────────────────────────────────────
  private val reserveNowEp = stationBase.post
    .in("Action" / "ReserveNow")
    .in(tenantHeader)
    .in(jsonBody[ReserveNowRequest])
    .out(jsonBody[CommandResult])

  // ── Cancel Reservation ────────────────────────────────────────────────────
  private val cancelReservationEp = stationBase.post
    .in("Action" / "CancelReservation")
    .in(tenantHeader)
    .in(jsonBody[CancelReservationRequest])
    .out(jsonBody[CommandResult])

  def routes(
      repo: RestApiRepository,
      gateway: OcppGatewayGrpcClient
  ): List[ZServerEndpoint[Any, Any]] =

    def send(tenantId: String, stationId: String, action: String, payloadJson: String) =
      gateway
        .sendCommand(tenantId, stationId, action, payloadJson)
        .map(r =>
          if r.errorCode.nonEmpty then CommandResult(r.errorCode, Some(r.errorMessage))
          else CommandResult("Accepted", None)
        )
        .mapError(e => s"gRPC error: ${e.getMessage}")

    List(
      resetEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "Reset", Map("type" -> req.resetType).asJson.noSpaces)
      },
      clearCacheEp.zServerLogic { case (stationId, tenantId) =>
        send(tenantId, stationId, "ClearCache", "{}")
      },
      remoteStartEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "RemoteStartTransaction", req.asJson.noSpaces)
      },
      remoteStopEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "RemoteStopTransaction", req.asJson.noSpaces)
      },
      unlockEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "UnlockConnector", req.asJson.noSpaces)
      },
      triggerMessageEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "TriggerMessage", req.asJson.noSpaces)
      },
      changeConfigEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "ChangeConfiguration", req.asJson.noSpaces)
      },
      getConfigEp.zServerLogic { case (stationId, tenantId) =>
        send(tenantId, stationId, "GetConfiguration", "{}").map { _ =>
          io.circe.Json.obj("configurationKey" -> io.circe.Json.arr())
        }
      },
      dataTransferEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        import io.circe.generic.auto._
        send(tenantId, stationId, "DataTransfer", req.asJson.noSpaces)
      },
      setChargingProfileEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "connectorId"     -> io.circe.Json.fromInt(req.connectorId),
          "chargingProfile" -> req.chargingProfile
        ).noSpaces
        send(tenantId, stationId, "SetChargingProfile", payload)
      },
      getCompositeScheduleEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "connectorId"      -> io.circe.Json.fromInt(req.connectorId),
          "duration"         -> io.circe.Json.fromInt(req.duration),
          "chargingRateUnit" -> req.chargingRateUnit.fold(io.circe.Json.Null)(io.circe.Json.fromString)
        ).noSpaces
        send(tenantId, stationId, "GetCompositeSchedule", payload).map { r =>
          io.circe.Json.obj("status" -> io.circe.Json.fromString(r.status))
        }
      },
      clearChargingProfileEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "id"                     -> req.id.fold(io.circe.Json.Null)(io.circe.Json.fromInt),
          "connectorId"            -> req.connectorId.fold(io.circe.Json.Null)(io.circe.Json.fromInt),
          "chargingProfilePurpose" -> req.chargingProfilePurpose.fold(io.circe.Json.Null)(io.circe.Json.fromString)
        ).noSpaces
        send(tenantId, stationId, "ClearChargingProfile", payload)
      },
      changeAvailabilityEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "connectorId" -> io.circe.Json.fromInt(req.connectorId),
          "type"        -> io.circe.Json.fromString(req.availabilityType)
        ).noSpaces
        send(tenantId, stationId, "ChangeAvailability", payload)
      },
      getDiagnosticsEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "location"      -> io.circe.Json.fromString(req.location),
          "retries"       -> req.retries.fold(io.circe.Json.Null)(io.circe.Json.fromInt),
          "retryInterval" -> req.retryInterval.fold(io.circe.Json.Null)(io.circe.Json.fromInt),
          "startTime"     -> req.startTime.fold(io.circe.Json.Null)(io.circe.Json.fromLong),
          "stopTime"      -> req.stopTime.fold(io.circe.Json.Null)(io.circe.Json.fromLong)
        ).noSpaces
        send(tenantId, stationId, "GetDiagnostics", payload)
      },
      updateFirmwareEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "location"      -> io.circe.Json.fromString(req.location),
          "retrieveDate"  -> io.circe.Json.fromLong(req.retrieveDate),
          "retries"       -> req.retries.fold(io.circe.Json.Null)(io.circe.Json.fromInt),
          "retryInterval" -> req.retryInterval.fold(io.circe.Json.Null)(io.circe.Json.fromInt)
        ).noSpaces
        send(tenantId, stationId, "UpdateFirmware", payload).as(())
      },
      reserveNowEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "connectorId"   -> io.circe.Json.fromInt(req.connectorId),
          "expiryDate"    -> io.circe.Json.fromLong(req.expiryDate),
          "idTag"         -> io.circe.Json.fromString(req.idTag),
          "reservationId" -> io.circe.Json.fromInt(req.reservationId)
        ).noSpaces
        send(tenantId, stationId, "ReserveNow", payload)
      },
      cancelReservationEp.zServerLogic { case (stationId, tenantId, req) =>
        import io.circe.syntax._
        val payload = io.circe.Json.obj(
          "reservationId" -> io.circe.Json.fromInt(req.reservationId)
        ).noSpaces
        send(tenantId, stationId, "CancelReservation", payload)
      }
    )
