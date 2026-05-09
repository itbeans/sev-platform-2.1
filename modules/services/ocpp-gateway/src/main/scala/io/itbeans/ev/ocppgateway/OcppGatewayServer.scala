package io.itbeans.ev.ocppgateway

import io.itbeans.ev.auth.AuthorizationService
import io.itbeans.ev.domain.{ChargingStationId, OcppVersion, TenantId}
import io.itbeans.ev.kafka.EvKafkaProducer
import zio._
import zio.http._
import zio.metrics.connectors.prometheus.PrometheusPublisher

// ---------------------------------------------------------------------------
// OcppGatewayServer — ZIO HTTP WebSocket server for OCPP connections.
//
// URL pattern (Kong routes subdomain → this service with injected headers):
//   wss://{tenantSubdomain}.domain/ocpp/2.1/{stationId}
//   wss://{tenantSubdomain}.domain/ocpp/2.0.1/{stationId}
//   wss://{tenantSubdomain}.domain/ocpp/1.6/{stationId}
//
// Kong responsibilities (before this handler):
//   1. Resolves subdomain → tenantId; injects X-Tenant-ID header
//   2. Validates JWT if present; injects X-User-ID header
//   3. Routes by stationId hash for sticky pod affinity
//
// Each station is one ZIO fiber. Connection lifetime:
//   connect → register → fiber loop → deregister → fiber exit
// ---------------------------------------------------------------------------

final class OcppGatewayServer(
    cfg: GatewayConfig,
    registry: ConnectionRegistry,
    frameHandler: OcppFrameHandler,
    producer: EvKafkaProducer,
    auth: AuthorizationService,
    metricsPublisher: PrometheusPublisher
):

  def start: Task[Unit] =
    ZIO.logInfo(s"ev-ocpp-gateway listening on ws=:${cfg.httpPort} grpc=:${cfg.grpcPort}") *>
      Server
        .serve(routes.handleError(err => Response.internalServerError(err.getMessage)))
        .provide(Server.defaultWithPort(cfg.httpPort))
        .unit

  private val routes: Routes[Any, Throwable] = Routes(
    // ── OCPP WebSocket endpoints ────────────────────────────────────────────
    Method.GET / "ocpp" / "2.1" / string("stationId")   -> upgradeHandler(OcppVersion.V2_1),
    Method.GET / "ocpp" / "2.0.1" / string("stationId") -> upgradeHandler(OcppVersion.V2_0_1),
    Method.GET / "ocpp" / "1.6" / string("stationId")   -> upgradeHandler(OcppVersion.V1_6),

    // ── Admin / platform endpoints ──────────────────────────────────────────
    Method.GET / "health" -> Handler.ok,
    Method.GET / "metrics" -> Handler.fromFunctionZIO[Request](_ =>
      metricsPublisher.get.map(text =>
        Response.text(text).addHeader(Header.ContentType(MediaType.text.plain))
      )
    ),
    Method.GET / "registry" -> Handler.fromFunctionZIO[Request](req =>
      val tenantId = extractTenantId(req)
      registry.listAll(tenantId.map(TenantId(_))).map { entries =>
        Response.json(
          s"""{"count":${entries.size},"stations":${entries.map(e => s""""${e.key.stationId.value}"""").mkString(
              "[",
              ",",
              "]"
            )}}"""
        )
      }
    )
  )

  // ── WebSocket upgrade handler ───────────────────────────────────────────

  private def upgradeHandler(ocppVersion: OcppVersion): Handler[Any, Throwable, (String, Request), Response] =
    Handler.fromFunctionZIO[(String, Request)] { case (stationIdStr, req) =>
      val stationId = ChargingStationId(stationIdStr)
      val tenantId  = TenantId(extractTenantId(req).getOrElse("default"))
      val key       = ConnectionKey(tenantId, stationId)

      Handler.webSocket { channel =>
        for
          // Register connection in global registry; deregister on fiber exit
          _ <- registry.register(
            ConnectionEntry(key, channel, ocppVersion, java.time.Instant.now())
          ).ensuring(registry.deregister(key))
          _ <- ZIO.logInfo(s"[Gateway] Station $stationId connected ($ocppVersion)")
          // Main receive loop — runs until WebSocket closes
          _ <- channel.receiveAll { event =>
            event match
              case ChannelEvent.Read(WebSocketFrame.Text(msg)) =>
                frameHandler.handle(msg, tenantId, stationId, ocppVersion, channel)
                  .tapError(err =>
                    ZIO.logError(s"[Gateway] Frame error from $stationId: ${err.getMessage}")
                  )
                  .ignore

              case ChannelEvent.Read(WebSocketFrame.Ping) =>
                channel.send(ChannelEvent.Read(WebSocketFrame.Pong))

              case ChannelEvent.Read(WebSocketFrame.Close(_, reason)) =>
                ZIO.logInfo(s"[Gateway] Station $stationId closed: ${reason.getOrElse("no reason")}")

              case _ => ZIO.unit
          }
          _ <- ZIO.logInfo(s"[Gateway] Station $stationId disconnected")
        yield ()
      }.toResponse
    }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private def extractTenantId(req: Request): Option[String] =
    req.headers.get("X-Tenant-ID")

object OcppGatewayServer:

  val live: ZLayer[
    GatewayConfig & ConnectionRegistry & OcppFrameHandler & EvKafkaProducer & AuthorizationService & PrometheusPublisher,
    Nothing,
    OcppGatewayServer
  ] =
    ZLayer.fromFunction(OcppGatewayServer(_, _, _, _, _, _))
