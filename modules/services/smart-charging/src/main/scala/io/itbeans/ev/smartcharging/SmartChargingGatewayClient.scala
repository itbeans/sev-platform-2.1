package io.itbeans.ev.smartcharging

import io.circe.syntax._
import io.circe.Json
import io.grpc.ManagedChannelBuilder
import io.itbeans.ev.ocpp.grpc.ocpp_gateway._
import zio._

import java.util.UUID

// ---------------------------------------------------------------------------
// SmartChargingGatewayClient — gRPC client to ev-ocpp-gateway.
//
// Delivers SetChargingProfile commands to charging stations after the SAP
// optimizer returns a plan.  Builds OCPP 2.1 SetChargingProfile payloads;
// the gateway translates to the station's negotiated OCPP version on the wire.
//
// The call is fire-and-forget with logged error — a failed delivery does not
// abort the optimization run; the profile is still persisted in MongoDB and
// the next periodic recompute will re-deliver it.
// ---------------------------------------------------------------------------

trait SmartChargingGatewayClient:
  def deliverChargingProfile(tenantId: String, profile: ChargingProfile): Task[Boolean]

final class LiveSmartChargingGatewayClient(
    stub: OcppGatewayServiceGrpc.OcppGatewayServiceStub
) extends SmartChargingGatewayClient:

  def deliverChargingProfile(tenantId: String, profile: ChargingProfile): Task[Boolean] =
    val payload = buildSetChargingProfilePayload(profile)
    ZIO.fromFuture(implicit ec =>
      stub.sendCommand(
        SendCommandRequest(
          tenantId = tenantId,
          chargingStationId = profile.chargingStationId,
          action = "SetChargingProfile",
          payloadJson = payload.noSpaces,
          correlationId = UUID.randomUUID().toString,
          timeoutSeconds = 30
        )
      )
    ).map(_.delivered)

  // Build OCPP 2.1 SetChargingProfile request payload.
  private def buildSetChargingProfilePayload(p: ChargingProfile): Json =
    val periods = p.profile.chargingSchedule.periods.map { period =>
      Json.obj(
        "startPeriod"  -> Json.fromInt(period.startPeriod),
        "limit"        -> Json.fromDouble(period.limitWatts).getOrElse(Json.fromInt(0)),
        "numberPhases" -> period.numberPhases.fold(Json.Null)(Json.fromInt)
      )
    }

    val schedule = Json.obj(
      "id"                     -> Json.fromInt(p.profile.chargingProfileId),
      "startSchedule"          -> p.profile.chargingSchedule.startSchedule.fold(Json.Null)(t => Json.fromString(t.toString)),
      "duration"               -> p.profile.chargingSchedule.durationSecs.fold(Json.Null)(Json.fromInt),
      "chargingRateUnit"       -> Json.fromString(p.profile.chargingSchedule.chargingRateUnit),
      "chargingSchedulePeriod" -> Json.arr(periods*)
    )

    Json.obj(
      "evseId" -> Json.fromInt(p.connectorId),
      "chargingProfile" -> Json.obj(
        "id"                     -> Json.fromInt(p.profile.chargingProfileId),
        "stackLevel"             -> Json.fromInt(p.profile.stackLevel),
        "chargingProfilePurpose" -> Json.fromString(p.profile.chargingProfilePurpose),
        "chargingProfileKind"    -> Json.fromString(p.profile.chargingProfileKind),
        "validFrom"              -> p.profile.validFrom.fold(Json.Null)(t => Json.fromString(t.toString)),
        "validTo"                -> p.profile.validTo.fold(Json.Null)(t => Json.fromString(t.toString)),
        "transactionId"          -> p.profile.transactionId.fold(Json.Null)(Json.fromLong),
        "chargingSchedule"       -> Json.arr(schedule)
      )
    )

object SmartChargingGatewayClient:

  val live: ZLayer[SmartChargingConfig, Throwable, SmartChargingGatewayClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[SmartChargingConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.gatewayGrpcHost, cfg.gatewayGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new LiveSmartChargingGatewayClient(OcppGatewayServiceGrpc.stub(channel))
    }
