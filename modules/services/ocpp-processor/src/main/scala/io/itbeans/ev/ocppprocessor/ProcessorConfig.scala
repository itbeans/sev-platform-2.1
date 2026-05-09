package io.itbeans.ev.ocppprocessor

import zio._

// ---------------------------------------------------------------------------
// ProcessorConfig — ZIO Config for ev-ocpp-processor.
// ---------------------------------------------------------------------------

case class ProcessorConfig(
    // MongoDB — shared with TypeScript monolith during transition
    mongoUri: String,
    mongoDbName: String,

    // gRPC addresses of upstream services
    authGrpcHost: String,
    authGrpcPort: Int,
    pricingGrpcHost: String,
    pricingGrpcPort: Int,
    gatewayGrpcHost: String,
    gatewayGrpcPort: Int,

    // Kafka consumer group — scale by increasing partition count
    consumerGroupId: String, // default "ev-ocpp-processor"

    // Heartbeat timeout: station offline after this many seconds without heartbeat
    stationOfflineThresholdSecs: Int // default 540 (9 min)
)

object ProcessorConfig:

  given Config[ProcessorConfig] = (
    Config.string("mongoUri") zip
      Config.string("mongoDbName") zip
      Config.string("authGrpcHost") zip
      Config.int("authGrpcPort") zip
      Config.string("pricingGrpcHost") zip
      Config.int("pricingGrpcPort") zip
      Config.string("gatewayGrpcHost") zip
      Config.int("gatewayGrpcPort") zip
      Config.string("consumerGroupId") zip
      Config.int("stationOfflineThresholdSecs")
  ).nested("processor").map {
    case (
          mongoUri,
          mongoDbName,
          authHost,
          authPort,
          pricingHost,
          pricingPort,
          gwHost,
          gwPort,
          groupId,
          offlineThresh
        ) =>
      ProcessorConfig(
        mongoUri,
        mongoDbName,
        authHost,
        authPort,
        pricingHost,
        pricingPort,
        gwHost,
        gwPort,
        groupId,
        offlineThresh
      )
  }
