package io.itbeans.ev.pricingservice

import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.pricing.grpc.pricing_service._
import zio._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Bridges the generated Future-based `PricingServiceGrpc.PricingService` to
 *  the ZIO-native `PricingGrpcHandler`.
 */
final class PricingGrpcTransport(handler: PricingGrpcHandler, rt: Runtime[Any])
    extends PricingServiceGrpc.PricingService:

  private def run[A](effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(effect))

  override def resolvePricing(req: ResolvePricingRequest): Future[ResolvePricingResponse] =
    run(
      handler
        .resolvePricing(
          ResolvePricingRequestADT(
            tenantId = req.tenantId,
            chargingStationId = req.chargingStationId,
            connectorId = req.connectorId,
            connectorType = "",     // not in proto — defaulted
            connectorPowerKw = 0.0, // not in proto — defaulted
            siteId = None,
            siteAreaId = None,
            companyId = None,
            userId = req.userId,
            startTimestamp = req.startTimestamp
          )
        )
        .map { r =>
          ResolvePricingResponse(
            found = r.found,
            pricingModelJson = r.pricingModelJson,
            currency = r.currency
          )
        }
    )

  override def priceConsumption(req: PriceConsumptionRequest): Future[PriceConsumptionResponse] =
    run(
      handler
        .priceConsumption(
          PriceConsumptionRequestADT(
            tenantId = req.tenantId,
            transactionId = req.transactionId,
            pricingModelJson = req.pricingModelJson,
            consumptionWh = req.consumptionWh,
            cumulatedWh = 0.0,     // not in proto
            totalDurationSecs = 0, // not in proto
            inactivitySecs = 0,
            totalInactivitySecs = 0,
            intervalStart = req.intervalStart,
            intervalEnd = req.intervalEnd,
            timezone = "UTC",
            connectorType = "",
            connectorPowerKw = req.instantWatts / 1000.0,
            flatFeeAlreadyPriced = false,
            cumulatedPrice = 0.0
          )
        )
        .map { r =>
          PriceConsumptionResponse(
            price = r.price,
            cumulatedPrice = r.cumulatedPrice,
            currency = r.currency
          )
        }
    )

  override def finalisePrice(req: FinalisePriceRequest): Future[FinalisePriceResponse] =
    run(
      handler
        .finalisePrice(
          FinalisePriceRequestADT(
            tenantId = req.tenantId,
            transactionId = req.transactionId,
            pricingModelJson = req.pricingModelJson,
            totalConsumptionWh = req.totalConsumptionWh,
            startTimestamp = req.startTimestamp,
            endTimestamp = req.endTimestamp,
            timezone = "UTC", // not in proto
            connectorType = "",
            connectorPowerKw = 0.0
          )
        )
        .map { r =>
          FinalisePriceResponse(
            totalPrice = r.totalPrice,
            currency = r.currency,
            invoiceItem = r.invoiceItem
          )
        }
    )

object PricingGrpcTransport:

  val start: RIO[PricingGrpcHandler & PricingConfig, Unit] =
    for
      handler <- ZIO.service[PricingGrpcHandler]
      cfg     <- ZIO.service[PricingConfig]
      rt      <- ZIO.runtime[Any]
      impl = new PricingGrpcTransport(handler, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(PricingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Pricing gRPC server listening on :${server.getPort}")
    yield ()
