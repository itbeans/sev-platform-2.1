package io.itbeans.ev.ocppgateway

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.itbeans.ev.domain.{ChargingStationId, TenantId}
import io.itbeans.ev.kafka.{EvKafkaProducer, KafkaConfig, Topics}
import io.itbeans.ev.ocpp.grpc.ocpp_gateway._
import zio._
import zio.http.{ChannelEvent, WebSocketFrame}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde

import java.util.UUID
import java.util.regex.Pattern

// ---------------------------------------------------------------------------
// CrossPodCommandRelay — Kafka-based fan-out relay for OCPP commands that
// target a station connected to a different gateway pod.
//
// Fan-out pattern: each pod joins with a unique consumer group ID so every
// pod receives every relay command and response. Only the pod whose local
// ConnectionRegistry holds the target station acts; others discard.
//
// Command flow (pod A → pod B → pod A):
//   1. Pod A gets gRPC SendCommand; station absent from local registry.
//   2. Pod A publishes to ocpp.commands.{tenantId}, keyed by stationId.
//   3. All pods consume; pod B finds station locally → executes OCPP CALL.
//   4. Pod B publishes result to ocpp.command_responses, keyed by correlationId.
//   5. All pods consume; pod A finds the pending Promise → resolves it.
// ---------------------------------------------------------------------------

final class CrossPodCommandRelay(
    registry: ConnectionRegistry,
    frameHandler: OcppFrameHandler,
    producer: EvKafkaProducer,
    pendingRelays: Ref[Map[String, Promise[Throwable, SendCommandResponse]]],
    cfg: GatewayConfig,
    kafkaCfg: KafkaConfig
):
  private val podGroupId = s"ocpp-gateway-relay-${UUID.randomUUID()}"

  /** Called by OcppGatewayGrpcTransport when the station is not locally connected. */
  def relay(req: SendCommandRequest): UIO[SendCommandResponse] =
    val correlationId = if req.correlationId.nonEmpty then req.correlationId
    else UUID.randomUUID().toString
    val timeoutMs =
      (if req.timeoutSeconds > 0 then req.timeoutSeconds * 1000L
       else cfg.responseTimeoutMs) + 3000L // extra headroom for Kafka round-trip

    for
      promise <- Promise.make[Throwable, SendCommandResponse]
      _       <- pendingRelays.update(_ + (correlationId -> promise))
      msg = Json.obj(
        "correlationId"     -> Json.fromString(correlationId),
        "tenantId"          -> Json.fromString(req.tenantId),
        "chargingStationId" -> Json.fromString(req.chargingStationId),
        "action"            -> Json.fromString(req.action),
        "payloadJson"       -> Json.fromString(req.payloadJson),
        "timeoutSeconds"    -> Json.fromInt(req.timeoutSeconds)
      )
      tenantId = TenantId(req.tenantId)
      _ <- producer.publish(Topics.ocppCommands(tenantId), req.chargingStationId, msg)
        .catchAll(err =>
          ZIO.logError(s"[CrossPodRelay] Failed to publish relay command: ${err.getMessage}")
        )
      result <- promise.await
        .timeout(Duration.fromMillis(timeoutMs))
        .flatMap {
          case None =>
            pendingRelays.update(_ - correlationId) *>
              ZIO.succeed(SendCommandResponse(
                delivered = false,
                errorCode = "RelayTimeout",
                errorMessage = s"No relay response within ${timeoutMs}ms"
              ))
          case Some(resp) => ZIO.succeed(resp)
        }
    yield result

  /** Consumes ocpp.commands.* — executes commands for locally-connected stations. */
  def startCommandConsumer: RIO[Scope, Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId(podGroupId + "-cmd")
      .withProperty("auto.offset.reset", kafkaCfg.autoOffsetReset)
    for
      consumer <- Consumer.make(settings)
      _ <- consumer
        .plainStream(
          Subscription.pattern(Pattern.compile("ocpp\\.commands\\..*")),
          Serde.string,
          Serde.string
        )
        .mapZIO { record =>
          handleInboundRelayCommand(record.value).ignore *>
            record.offset.commit
        }
        .runDrain
    yield ()

  /** Consumes ocpp.command_responses — resolves Promises for relayed commands. */
  def startResponseConsumer: RIO[Scope, Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId(podGroupId + "-rsp")
      .withProperty("auto.offset.reset", kafkaCfg.autoOffsetReset)
    for
      consumer <- Consumer.make(settings)
      _ <- consumer
        .plainStream(
          Subscription.topics(Topics.ocppCommandResponses),
          Serde.string,
          Serde.string
        )
        .mapZIO { record =>
          handleRelayResponse(record.value).ignore *>
            record.offset.commit
        }
        .runDrain
    yield ()

  // ── Inbound command handling (on all pods, act only if station is local) ──

  private def handleInboundRelayCommand(json: String): Task[Unit] =
    for
      obj <- ZIO.fromEither(parse(json)).mapError(e => new Exception(e.message))
      cursor = obj.hcursor
      correlationId <- ZIO.fromEither(cursor.get[String]("correlationId"))
      tenantId      <- ZIO.fromEither(cursor.get[String]("tenantId"))
      stationId     <- ZIO.fromEither(cursor.get[String]("chargingStationId"))
      action        <- ZIO.fromEither(cursor.get[String]("action"))
      payloadJson   <- ZIO.fromEither(cursor.get[String]("payloadJson"))
      timeoutSecs = cursor.get[Int]("timeoutSeconds").getOrElse(0)
      key         = ConnectionKey(TenantId(tenantId), ChargingStationId(stationId))
      entryOpt <- registry.get(key)
      _ <- entryOpt match
        case None => ZIO.unit // station not on this pod — another pod will handle it
        case Some(_) =>
          executeLocalAndPublishResponse(
            key,
            correlationId,
            action,
            payloadJson,
            timeoutSecs
          )
    yield ()

  private def executeLocalAndPublishResponse(
      key: ConnectionKey,
      correlationId: String,
      action: String,
      payloadJson: String,
      timeoutSecs: Int
  ): Task[Unit] =
    registry.get(key).flatMap {
      case None => ZIO.unit
      case Some(entry) =>
        val timeoutMs = if timeoutSecs > 0 then timeoutSecs * 1000L else cfg.responseTimeoutMs
        val payload   = parse(payloadJson).getOrElse(Json.obj())
        val frame = Json.arr(
          Json.fromInt(2),
          Json.fromString(correlationId),
          Json.fromString(action),
          payload
        ).noSpaces

        for
          promise <- Promise.make[Throwable, OcppFrame]
          _       <- frameHandler.registerPending(correlationId, action, promise)
          sent <- entry.channel
            .send(ChannelEvent.Read(WebSocketFrame.Text(frame)))
            .tapError(_ => frameHandler.removePending(correlationId))
            .either
          responseMsg <- sent match
            case Left(err) =>
              ZIO.succeed(buildResponseMsg(
                correlationId,
                delivered = false,
                errorCode = "SendError",
                errorMessage = err.getMessage,
                responseJson = ""
              ))
            case Right(_) =>
              promise.await
                .timeout(Duration.fromMillis(timeoutMs))
                .flatMap {
                  case None =>
                    frameHandler.removePending(correlationId) *>
                      ZIO.succeed(buildResponseMsg(
                        correlationId,
                        delivered = false,
                        errorCode = "Timeout",
                        errorMessage = s"No response from station within ${timeoutMs}ms",
                        responseJson = ""
                      ))
                  case Some(ocppFrame) => ocppFrame match
                      case r: OcppCallResultFrame =>
                        ZIO.succeed(buildResponseMsg(correlationId, delivered = true, responseJson = r.payloadJson))
                      case e: OcppCallErrorFrame =>
                        ZIO.succeed(buildResponseMsg(
                          correlationId,
                          delivered = true,
                          errorCode = e.errorCode,
                          errorMessage = e.errorDescription,
                          responseJson = ""
                        ))
                      case _ =>
                        ZIO.succeed(buildResponseMsg(
                          correlationId,
                          delivered = false,
                          errorCode = "UnexpectedFrame",
                          responseJson = ""
                        ))
                }
          _ <- producer.publish(Topics.ocppCommandResponses, correlationId, responseMsg)
        yield ()
    }

  private def buildResponseMsg(
      correlationId: String,
      delivered: Boolean,
      errorCode: String = "",
      errorMessage: String = "",
      responseJson: String = ""
  ): Json =
    Json.obj(
      "correlationId" -> Json.fromString(correlationId),
      "delivered"     -> Json.fromBoolean(delivered),
      "errorCode"     -> Json.fromString(errorCode),
      "errorMessage"  -> Json.fromString(errorMessage),
      "responseJson"  -> Json.fromString(responseJson)
    )

  // ── Inbound response handling (resolve the pending Promise on the origin pod) ─

  private def handleRelayResponse(json: String): Task[Unit] =
    for
      obj <- ZIO.fromEither(parse(json)).mapError(e => new Exception(e.message))
      cursor = obj.hcursor
      correlationId <- ZIO.fromEither(cursor.get[String]("correlationId"))
      promiseOpt    <- pendingRelays.get.map(_.get(correlationId))
      _ <- promiseOpt match
        case None => ZIO.unit // this correlationId belongs to another pod
        case Some(promise) =>
          val resp = SendCommandResponse(
            delivered = cursor.get[Boolean]("delivered").getOrElse(false),
            errorCode = cursor.get[String]("errorCode").getOrElse(""),
            errorMessage = cursor.get[String]("errorMessage").getOrElse(""),
            responseJson = cursor.get[String]("responseJson").getOrElse("")
          )
          pendingRelays.update(_ - correlationId) *>
            promise.succeed(resp).unit
    yield ()

object CrossPodCommandRelay:

  val live: ZLayer[
    ConnectionRegistry & OcppFrameHandler & EvKafkaProducer & GatewayConfig & KafkaConfig,
    Nothing,
    CrossPodCommandRelay
  ] =
    ZLayer.fromZIO {
      for
        registry     <- ZIO.service[ConnectionRegistry]
        frameHandler <- ZIO.service[OcppFrameHandler]
        producer     <- ZIO.service[EvKafkaProducer]
        cfg          <- ZIO.service[GatewayConfig]
        kafkaCfg     <- ZIO.service[KafkaConfig]
        pending      <- Ref.make(Map.empty[String, Promise[Throwable, SendCommandResponse]])
      yield new CrossPodCommandRelay(registry, frameHandler, producer, pending, cfg, kafkaCfg)
    }
