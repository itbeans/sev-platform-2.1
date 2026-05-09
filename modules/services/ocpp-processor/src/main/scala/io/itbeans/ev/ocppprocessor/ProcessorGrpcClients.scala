package io.itbeans.ev.ocppprocessor

import io.grpc.ManagedChannelBuilder
import io.itbeans.ev.auth.grpc.auth_service._
import io.itbeans.ev.ocpp.grpc.ocpp_gateway._
import io.itbeans.ev.pricing.grpc.pricing_service._
import zio._

// ---------------------------------------------------------------------------
// gRPC client wrappers for ev-ocpp-processor.
//
// ProcessorAuthClient    — calls ev-auth-service to resolve OCPP Authorize
// ProcessorGatewayClient — calls ev-ocpp-gateway to send CALLRESULT back
// ---------------------------------------------------------------------------

// ── Auth client ───────────────────────────────────────────────────────────────

trait ProcessorAuthClient:
  def resolveOcppAuthorization(tenantId: String, idTag: String, stationId: String): Task[OcppAuthorizationResponse]

final class LiveProcessorAuthClient(stub: AuthServiceGrpc.AuthServiceStub) extends ProcessorAuthClient:

  def resolveOcppAuthorization(
      tenantId: String,
      idTag: String,
      stationId: String
  ): Task[OcppAuthorizationResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.resolveOcppAuthorization(
        OcppAuthorizationRequest(
          tenantId = tenantId,
          idTag = idTag,
          idTokenType = "ISO14443",
          chargingStationId = stationId
        )
      )
    )

object ProcessorAuthClient:

  val live: ZLayer[ProcessorConfig, Nothing, ProcessorAuthClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[ProcessorConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.authGrpcHost, cfg.authGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new LiveProcessorAuthClient(AuthServiceGrpc.stub(channel))
    }.orDie

// ── Gateway client ────────────────────────────────────────────────────────────

trait ProcessorGatewayClient:
  def sendCommand(tenantId: String, stationId: String, action: String, payloadJson: String): Task[SendCommandResponse]
  def sendResponse(tenantId: String, stationId: String, uniqueId: String, responseJson: String): Task[SendResponseAck]

final class LiveProcessorGatewayClient(stub: OcppGatewayServiceGrpc.OcppGatewayServiceStub)
    extends ProcessorGatewayClient:

  def sendCommand(
      tenantId: String,
      stationId: String,
      action: String,
      payloadJson: String
  ): Task[SendCommandResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.sendCommand(
        SendCommandRequest(
          tenantId = tenantId,
          chargingStationId = stationId,
          action = action,
          payloadJson = payloadJson,
          correlationId = java.util.UUID.randomUUID().toString,
          timeoutSeconds = 30
        )
      )
    )

  def sendResponse(
      tenantId: String,
      stationId: String,
      uniqueId: String,
      responseJson: String
  ): Task[SendResponseAck] =
    ZIO.fromFuture(implicit ec =>
      stub.sendResponse(
        SendResponseRequest(
          tenantId = tenantId,
          chargingStationId = stationId,
          uniqueId = uniqueId,
          responseJson = responseJson
        )
      )
    )

object ProcessorGatewayClient:

  val live: ZLayer[ProcessorConfig, Nothing, ProcessorGatewayClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[ProcessorConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.gatewayGrpcHost, cfg.gatewayGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new LiveProcessorGatewayClient(OcppGatewayServiceGrpc.stub(channel))
    }.orDie

// ── Pricing client ────────────────────────────────────────────────────────────

trait ProcessorPricingClient:
  def resolvePricing(tenantId: String, stationId: String, connectorId: String, userId: String, startTimestampMs: Long): Task[ResolvePricingResponse]
  def priceConsumption(tenantId: String, txId: String, pricingModelJson: String, consumptionWh: Double, instantWatts: Double, intervalStartMs: Long, intervalEndMs: Long): Task[PriceConsumptionResponse]
  def finalisePrice(tenantId: String, txId: String, pricingModelJson: String, totalConsumptionWh: Double, startTimestampMs: Long, endTimestampMs: Long): Task[FinalisePriceResponse]

final class LiveProcessorPricingClient(stub: PricingServiceGrpc.PricingServiceStub)
    extends ProcessorPricingClient:

  def resolvePricing(
      tenantId: String,
      stationId: String,
      connectorId: String,
      userId: String,
      startTimestampMs: Long
  ): Task[ResolvePricingResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.resolvePricing(
        ResolvePricingRequest(
          tenantId = tenantId,
          chargingStationId = stationId,
          connectorId = connectorId,
          userId = userId,
          tagId = userId,
          startTimestamp = startTimestampMs
        )
      )
    )

  def priceConsumption(
      tenantId: String,
      txId: String,
      pricingModelJson: String,
      consumptionWh: Double,
      instantWatts: Double,
      intervalStartMs: Long,
      intervalEndMs: Long
  ): Task[PriceConsumptionResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.priceConsumption(
        PriceConsumptionRequest(
          tenantId = tenantId,
          transactionId = txId,
          pricingModelJson = pricingModelJson,
          consumptionWh = consumptionWh,
          instantWatts = instantWatts,
          intervalStart = intervalStartMs,
          intervalEnd = intervalEndMs
        )
      )
    )

  def finalisePrice(
      tenantId: String,
      txId: String,
      pricingModelJson: String,
      totalConsumptionWh: Double,
      startTimestampMs: Long,
      endTimestampMs: Long
  ): Task[FinalisePriceResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.finalisePrice(
        FinalisePriceRequest(
          tenantId = tenantId,
          transactionId = txId,
          pricingModelJson = pricingModelJson,
          totalConsumptionWh = totalConsumptionWh,
          startTimestamp = startTimestampMs,
          endTimestamp = endTimestampMs
        )
      )
    )

object ProcessorPricingClient:

  val live: ZLayer[ProcessorConfig, Nothing, ProcessorPricingClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[ProcessorConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.pricingGrpcHost, cfg.pricingGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new LiveProcessorPricingClient(PricingServiceGrpc.stub(channel))
    }.orDie
