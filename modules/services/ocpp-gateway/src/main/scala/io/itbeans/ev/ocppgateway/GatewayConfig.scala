package io.itbeans.ev.ocppgateway

import zio._

// ---------------------------------------------------------------------------
// GatewayConfig — ZIO Config for ev-ocpp-gateway.
// ---------------------------------------------------------------------------

case class GatewayConfig(
    httpPort: Int, // WebSocket listener port (default 8010)
    grpcPort: Int, // gRPC server port for SendCommand RPC (default 9090)

    // JWT validation
    jwtSecret: String,

    // Auth Service gRPC (to validate charging station tokens on Authorize)
    authGrpcHost: String,
    authGrpcPort: Int,

    // Processor response timeout (ms) — how long to wait for the OCPP processor
    // to send back the OCPP response frame before timing out
    responseTimeoutMs: Long, // default 30000 (30 s, per OCPP spec)

    // Connection health check interval
    heartbeatIntervalSecs: Int // default 60
)

object GatewayConfig:

  given Config[GatewayConfig] = (
    Config.int("httpPort") zip
      Config.int("grpcPort") zip
      Config.string("jwtSecret") zip
      Config.string("authGrpcHost") zip
      Config.int("authGrpcPort") zip
      Config.long("responseTimeoutMs") zip
      Config.int("heartbeatIntervalSecs")
  ).nested("gateway").map {
    case (httpPort, grpcPort, jwtSecret, authHost, authPort, responseTimeout, heartbeat) =>
      GatewayConfig(httpPort, grpcPort, jwtSecret, authHost, authPort, responseTimeout, heartbeat)
  }
