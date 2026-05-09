package io.itbeans.ev.ocppgateway

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.itbeans.ev.domain.{ChargingStationId, OcppVersion, TenantId}
import io.itbeans.ev.kafka.{EvKafkaProducer, Topics}
import zio._
import zio.http.WebSocketChannel
import zio.http.WebSocketFrame
import zio.metrics.Metric

// ---------------------------------------------------------------------------
// OcppFrameHandler — parses raw OCPP JSON frames and routes to Kafka.
//
// OCPP message frame format (JSON array):
//   [2, uniqueId, action, payload]          — CALL
//   [3, uniqueId, payload]                  — CALLRESULT
//   [4, uniqueId, code, description, det.]  — CALLERROR
//   [5, uniqueId, code, description, det.]  — CALLRESULTERROR (OCPP 2.1)
//   [6, uniqueId, action, payload]          — SEND (OCPP 2.1, one-way)
//
// On CALL/SEND: publish OcppCallEvent to Kafka → ev-ocpp-processor picks up,
//   processes, and sends back the OCPP response via the pending-response registry.
// On CALLRESULT/CALLERROR: complete the pending ZIO Promise for that uniqueId.
// ---------------------------------------------------------------------------

/** Per-station in-flight request tracker: uniqueId → Promise[OcppFrame]. */
case class PendingRequest(
    uniqueId: String,
    action: String,
    promise: Promise[Throwable, OcppFrame]
)

final class OcppFrameHandler(
    producer: EvKafkaProducer,
    pending: Ref[Map[String, PendingRequest]] // in-flight CALL requests from gateway→station
):

  // ── Pending-request management (used by OcppGatewayGrpcTransport) ──────────

  def registerPending(uniqueId: String, action: String, promise: Promise[Throwable, OcppFrame]): UIO[Unit] =
    pending.update(_ + (uniqueId -> PendingRequest(uniqueId, action, promise)))

  def removePending(uniqueId: String): UIO[Unit] =
    pending.update(_ - uniqueId)

  def handle(
      rawFrame: String,
      tenantId: TenantId,
      stationId: ChargingStationId,
      version: OcppVersion,
      channel: WebSocketChannel
  ): Task[Unit] =
    for
      frame <- parseFrame(rawFrame)
      _ <- frame match
        case call: OcppCallFrame         => handleCall(call, tenantId, stationId, version)
        case result: OcppCallResultFrame => handleCallResult(result, tenantId, stationId)
        case error: OcppCallErrorFrame   => handleCallError(error, tenantId, stationId)
        case send: OcppSendFrame         => handleSend(send, tenantId, stationId, version)
        case _: OcppCallResultErrorFrame => ZIO.logWarning(s"CALLRESULTERROR from $stationId")
    yield ()

  // ── Frame parsing ────────────────────────────────────────────────────────

  // Package-private for testing
  private[ocppgateway] def parseFramePublic(raw: String): Task[OcppFrame] = parseFrame(raw)

  private def parseFrame(raw: String): Task[OcppFrame] =
    ZIO.fromEither(parse(raw).flatMap { json =>
      json.asArray.toRight(DecodingFailure("Expected JSON array", Nil)).flatMap { arr =>
        arr.headOption.flatMap(_.asNumber).flatMap(_.toInt) match
          case Some(2) => parseCall(arr)
          case Some(3) => parseCallResult(arr)
          case Some(4) => parseCallError(arr)
          case Some(5) => parseCallResultError(arr)
          case Some(6) => parseSend(arr)
          case t       => Left(DecodingFailure(s"Unknown message type: $t", Nil))
      }
    }).mapError(e => new Exception(s"Failed to parse OCPP frame: ${e.getMessage}"))

  private def parseCall(arr: Vector[Json]): Either[DecodingFailure, OcppCallFrame] =
    for
      uniqueId <- arr.lift(1).flatMap(_.asString).toRight(DecodingFailure("Missing uniqueId in CALL", Nil))
      action   <- arr.lift(2).flatMap(_.asString).toRight(DecodingFailure("Missing action in CALL", Nil))
      payload = arr.lift(3).getOrElse(Json.obj()).noSpaces
    yield OcppCallFrame(uniqueId, action, payload)

  private def parseCallResult(arr: Vector[Json]): Either[DecodingFailure, OcppCallResultFrame] =
    for
      uniqueId <- arr.lift(1).flatMap(_.asString).toRight(DecodingFailure("Missing uniqueId in CALLRESULT", Nil))
      payload = arr.lift(2).getOrElse(Json.obj()).noSpaces
    yield OcppCallResultFrame(uniqueId, payload)

  private def parseCallError(arr: Vector[Json]): Either[DecodingFailure, OcppCallErrorFrame] =
    for
      uniqueId  <- arr.lift(1).flatMap(_.asString).toRight(DecodingFailure("Missing uniqueId in CALLERROR", Nil))
      errorCode <- arr.lift(2).flatMap(_.asString).toRight(DecodingFailure("Missing errorCode in CALLERROR", Nil))
      description = arr.lift(3).flatMap(_.asString).getOrElse("")
    yield OcppCallErrorFrame(uniqueId, errorCode, description)

  private def parseCallResultError(arr: Vector[Json]): Either[DecodingFailure, OcppCallResultErrorFrame] =
    for
      uniqueId  <- arr.lift(1).flatMap(_.asString).toRight(DecodingFailure("Missing uniqueId", Nil))
      errorCode <- arr.lift(2).flatMap(_.asString).toRight(DecodingFailure("Missing errorCode", Nil))
      description = arr.lift(3).flatMap(_.asString).getOrElse("")
    yield OcppCallResultErrorFrame(uniqueId, errorCode, description)

  private def parseSend(arr: Vector[Json]): Either[DecodingFailure, OcppSendFrame] =
    for
      uniqueId <- arr.lift(1).flatMap(_.asString).toRight(DecodingFailure("Missing uniqueId in SEND", Nil))
      action   <- arr.lift(2).flatMap(_.asString).toRight(DecodingFailure("Missing action in SEND", Nil))
      payload = arr.lift(3).getOrElse(Json.obj()).noSpaces
    yield OcppSendFrame(uniqueId, action, payload)

  private val framesReceived = Metric.counter("ocpp_frames_received_total")

  // ── CALL → Kafka ─────────────────────────────────────────────────────────

  private def handleCall(
      frame: OcppCallFrame,
      tenantId: TenantId,
      stationId: ChargingStationId,
      version: OcppVersion
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"    -> Json.fromString(tenantId.value),
      "stationId"   -> Json.fromString(stationId.value),
      "version"     -> Json.fromString(version.toString),
      "messageType" -> Json.fromInt(2),
      "uniqueId"    -> Json.fromString(frame.uniqueId),
      "action"      -> Json.fromString(frame.action),
      "payload"     -> parse(frame.payloadJson).getOrElse(Json.obj())
    )
    producer.publish(Topics.ocppEvents(tenantId), stationId.value, event) *>
      framesReceived.tagged("action", frame.action).tagged("type", "CALL").increment *>
      ZIO.logDebug(s"[Gateway] Published CALL ${frame.action} from $stationId (${frame.uniqueId})")

  // ── CALLRESULT → resolve pending ─────────────────────────────────────────

  private def handleCallResult(
      frame: OcppCallResultFrame,
      tenantId: TenantId,
      stationId: ChargingStationId
  ): Task[Unit] =
    pending.get.flatMap { map =>
      map.get(frame.uniqueId) match
        case None =>
          ZIO.logWarning(s"[Gateway] No pending request for CALLRESULT ${frame.uniqueId} from $stationId")
        case Some(req) =>
          req.promise.succeed(frame) *>
            pending.update(_ - frame.uniqueId) *>
            ZIO.logDebug(s"[Gateway] Resolved CALLRESULT for ${req.action} from $stationId")
    }

  private def handleCallError(
      frame: OcppCallErrorFrame,
      tenantId: TenantId,
      stationId: ChargingStationId
  ): Task[Unit] =
    pending.get.flatMap { map =>
      map.get(frame.uniqueId) match
        case None =>
          ZIO.logWarning(s"[Gateway] No pending for CALLERROR ${frame.uniqueId}")
        case Some(req) =>
          req.promise.fail(new Exception(s"OCPP error ${frame.errorCode}: ${frame.errorDescription}")) *>
            pending.update(_ - frame.uniqueId)
    }

  /** OCPP 2.1 SEND — publish to Kafka, no response required. */
  private def handleSend(
      frame: OcppSendFrame,
      tenantId: TenantId,
      stationId: ChargingStationId,
      version: OcppVersion
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"    -> Json.fromString(tenantId.value),
      "stationId"   -> Json.fromString(stationId.value),
      "version"     -> Json.fromString(version.toString),
      "messageType" -> Json.fromInt(6),
      "uniqueId"    -> Json.fromString(frame.uniqueId),
      "action"      -> Json.fromString(frame.action),
      "payload"     -> parse(frame.payloadJson).getOrElse(Json.obj())
    )
    producer.publish(Topics.ocppEvents(tenantId), stationId.value, event) *>
      framesReceived.tagged("action", frame.action).tagged("type", "SEND").increment *>
      ZIO.logDebug(s"[Gateway] Published SEND ${frame.action} from $stationId (no reply expected)")

object OcppFrameHandler:

  val live: ZLayer[EvKafkaProducer, Nothing, OcppFrameHandler] =
    ZLayer.fromZIO {
      for
        producer <- ZIO.service[EvKafkaProducer]
        pending  <- Ref.make(Map.empty[String, PendingRequest])
      yield new OcppFrameHandler(producer, pending)
    }

// Frame ADT
sealed trait OcppFrame
case class OcppCallFrame(uniqueId: String, action: String, payloadJson: String)                    extends OcppFrame
case class OcppCallResultFrame(uniqueId: String, payloadJson: String)                              extends OcppFrame
case class OcppCallErrorFrame(uniqueId: String, errorCode: String, errorDescription: String)       extends OcppFrame
case class OcppCallResultErrorFrame(uniqueId: String, errorCode: String, errorDescription: String) extends OcppFrame
case class OcppSendFrame(uniqueId: String, action: String, payloadJson: String)                    extends OcppFrame
