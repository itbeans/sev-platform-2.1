package io.itbeans.ev.smartcharging

import zio._
import zio.config.magnolia._

// ---------------------------------------------------------------------------
// SmartChargingConfig — ZIO Config for ev-smart-charging service.
// ---------------------------------------------------------------------------

case class SmartChargingConfig(
    httpPort: Int,
    grpcPort: Int,
    // SAP Smart Charging API
    optimizerUrl: String, // e.g. "https://sap-optimizer.example.com/api/v1/profiles"
    optimizerUser: String,
    optimizerPassword: String,
    // Behaviour
    debounceSecs: Int,         // de-duplicate triggers within this window (default 5)
    periodicIntervalMins: Int, // fallback periodic recompute interval (default 15)
    defaultVoltage: Int,       // V — used when site area has no voltage configured
    defaultNumberPhases: Int,  // default number of phases per connector
    // Tenants to watch (empty = all tenants via Kafka headers)
    defaultTenantId: String,
    // OCPP Gateway gRPC endpoint — for SetChargingProfile delivery
    gatewayGrpcHost: String,
    gatewayGrpcPort: Int
)

object SmartChargingConfig:

  given Config[SmartChargingConfig] = (
    Config.int("httpPort") zip
      Config.int("grpcPort") zip
      Config.string("optimizerUrl") zip
      Config.string("optimizerUser") zip
      Config.string("optimizerPassword") zip
      Config.int("debounceSecs") zip
      Config.int("periodicIntervalMins") zip
      Config.int("defaultVoltage") zip
      Config.int("defaultNumberPhases") zip
      Config.string("defaultTenantId") zip
      Config.string("gatewayGrpcHost") zip
      Config.int("gatewayGrpcPort")
  ).nested("smartCharging").map {
    case (httpPort, grpcPort, optimUrl, optimUser, optimPass, debounce, periodic, voltage, phases, tenantId, gwHost, gwPort) =>
      SmartChargingConfig(
        httpPort,
        grpcPort,
        optimUrl,
        optimUser,
        optimPass,
        debounce,
        periodic,
        voltage,
        phases,
        tenantId,
        gwHost,
        gwPort
      )
  }
