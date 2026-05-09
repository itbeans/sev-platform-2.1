package io.itbeans.ev.ocppgateway

import io.circe.parser.parse
import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.domain.{ChargingStationId, TenantId}
import io.itbeans.ev.ocpp.grpc.ocpp_gateway._
import zio._
import zio.http.{ChannelEvent, WebSocketFrame}

import scala.concurrent.{ExecutionContext, Future}

// ---------------------------------------------------------------------------
// OcppGatewayGrpcTransport — bridges generated Future-based gRPC service trait
// to ZIO effects backed by ConnectionRegistry + OcppFrameHandler.
//
// SendCommand  — sends a new OCPP CALL to the station and awaits CALLRESULT.
// SendResponse — sends an OCPP CALLRESULT for a station-originated CALL.
// ListConnectedStations / IsStationConnected — registry queries.
// ---------------------------------------------------------------------------

final class OcppGatewayGrpcTransport(
    registry: ConnectionRegistry,
    frameHandler: OcppFrameHandler,
    cfg: GatewayConfig,
    rt: Runtime[Any]
) extends OcppGatewayServiceGrpc.OcppGatewayService:

  private def run[A](effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(effect))

  override def sendCommand(req: SendCommandRequest): Future[SendCommandResponse] =
    run(doSendCommand(req))

  override def sendResponse(req: SendResponseRequest): Future[SendResponseAck] =
    run(doSendResponse(req))

  override def listConnectedStations(
      req: ListConnectedStationsRequest
  ): Future[ListConnectedStationsResponse] =
    run(doListConnectedStations(req))

  override def isStationConnected(
      req: IsStationConnectedRequest
  ): Future[IsStationConnectedResponse] =
    run(doIsStationConnected(req))

  // ── SendCommand ───────────────────────────────────────────────────────────
  // Sends a new OCPP CALL frame to the station and waits for the CALLRESULT.

  private def doSendCommand(req: SendCommandRequest): UIO[SendCommandResponse] =
    val key = ConnectionKey(TenantId(req.tenantId), ChargingStationId(req.chargingStationId))
    val uniqueId = if req.correlationId.nonEmpty then req.correlationId
    else java.util.UUID.randomUUID().toString
    val timeoutMs = if req.timeoutSeconds > 0 then req.timeoutSeconds * 1000L
    else cfg.responseTimeoutMs

    registry.get(key).flatMap {
      case None =>
        ZIO.succeed(SendCommandResponse(
          delivered = false,
          errorCode = "NotConnected",
          errorMessage = "Station not connected on this pod"
        ))

      case Some(entry) =>
        val payload = parse(req.payloadJson).getOrElse(io.circe.Json.obj())
        val frame = io.circe.Json.arr(
          io.circe.Json.fromInt(2),
          io.circe.Json.fromString(uniqueId),
          io.circe.Json.fromString(req.action),
          payload
        ).noSpaces

        for
          promise <- Promise.make[Throwable, OcppFrame]
          _       <- frameHandler.registerPending(uniqueId, req.action, promise)
          sent <- entry.channel
            .send(ChannelEvent.Read(WebSocketFrame.Text(frame)))
            .tapError(_ => frameHandler.removePending(uniqueId))
            .either
          result <- sent match
            case Left(err) =>
              ZIO.succeed(SendCommandResponse(
                delivered = false,
                errorCode = "SendError",
                errorMessage = err.getMessage
              ))
            case Right(_) =>
              promise.await
                .timeout(Duration.fromMillis(timeoutMs))
                .flatMap {
                  case None =>
                    frameHandler.removePending(uniqueId) *>
                      ZIO.succeed(SendCommandResponse(
                        delivered = false,
                        errorCode = "Timeout",
                        errorMessage = s"No response from station within ${timeoutMs}ms"
                      ))
                  case Some(ocppFrame) => ocppFrame match
                      case r: OcppCallResultFrame =>
                        ZIO.succeed(SendCommandResponse(delivered = true, responseJson = r.payloadJson))
                      case e: OcppCallErrorFrame =>
                        ZIO.succeed(SendCommandResponse(
                          delivered = true,
                          errorCode = e.errorCode,
                          errorMessage = e.errorDescription
                        ))
                      case _ =>
                        ZIO.succeed(SendCommandResponse(
                          delivered = false,
                          errorCode = "UnexpectedFrame"
                        ))
                }
        yield result

    }.catchAll(err =>
      ZIO.succeed(SendCommandResponse(
        delivered = false,
        errorCode = "Error",
        errorMessage = err.getMessage
      ))
    )

  // ── SendResponse ──────────────────────────────────────────────────────────
  // Sends an OCPP CALLRESULT frame back to the station for a CALL it made.
  // The uniqueId must match the original CALL uniqueId from the Kafka event.

  private def doSendResponse(req: SendResponseRequest): UIO[SendResponseAck] =
    val key = ConnectionKey(TenantId(req.tenantId), ChargingStationId(req.chargingStationId))
    registry.get(key).flatMap {
      case None =>
        ZIO.succeed(SendResponseAck(delivered = false))
      case Some(entry) =>
        val payload = parse(req.responseJson).getOrElse(io.circe.Json.obj())
        val frame = io.circe.Json.arr(
          io.circe.Json.fromInt(3),
          io.circe.Json.fromString(req.uniqueId),
          payload
        ).noSpaces
        entry.channel
          .send(ChannelEvent.Read(WebSocketFrame.Text(frame)))
          .as(SendResponseAck(delivered = true))
          .catchAll(_ => ZIO.succeed(SendResponseAck(delivered = false)))
    }

  // ── Registry queries ─────────────────────────────────────────────────────

  private def doListConnectedStations(
      req: ListConnectedStationsRequest
  ): UIO[ListConnectedStationsResponse] =
    val filter = if req.tenantId.isEmpty then None else Some(TenantId(req.tenantId))
    registry.listAll(filter).map { entries =>
      val stations = entries.map(e =>
        ConnectedStation(
          tenantId = e.key.tenantId.value,
          chargingStationId = e.key.stationId.value,
          ocppVersion = e.ocppVersion.toString,
          connectedSince = e.connectedAt.toEpochMilli
        )
      )
      ListConnectedStationsResponse(stations = stations)
    }

  private def doIsStationConnected(
      req: IsStationConnectedRequest
  ): UIO[IsStationConnectedResponse] =
    val key = ConnectionKey(TenantId(req.tenantId), ChargingStationId(req.chargingStationId))
    registry.get(key).map(opt => IsStationConnectedResponse(connected = opt.isDefined))

object OcppGatewayGrpcTransport:

  val start: RIO[ConnectionRegistry & OcppFrameHandler & GatewayConfig, Unit] =
    for
      registry     <- ZIO.service[ConnectionRegistry]
      frameHandler <- ZIO.service[OcppFrameHandler]
      cfg          <- ZIO.service[GatewayConfig]
      rt           <- ZIO.runtime[Any]
      impl = new OcppGatewayGrpcTransport(registry, frameHandler, cfg, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(OcppGatewayServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"[Gateway] gRPC server listening on :${server.getPort}")
    yield ()
