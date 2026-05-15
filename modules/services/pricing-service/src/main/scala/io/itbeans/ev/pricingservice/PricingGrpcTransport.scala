package io.itbeans.ev.pricingservice

import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.otel.EvTracing
import io.itbeans.ev.pricing.grpc.pricing_service._
import zio._

import scala.concurrent.{ExecutionContext, Future}

final class PricingGrpcTransport(handler: PricingGrpcHandler, tracing: EvTracing, rt: Runtime[Any])
    extends PricingServiceGrpc.PricingService:

  private def run[A](spanName: String)(effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(tracing.spanTask(spanName)(effect)))

  override def resolvePricing(req: ResolvePricingRequest): Future[ResolvePricingResponse] =
    run("pricing.resolvePricing")(
      handler
        .resolvePricing(
          ResolvePricingRequestADT(
            tenantId = req.tenantId,
            chargingStationId = req.chargingStationId,
            connectorId = req.connectorId,
            connectorType = req.connectorType,
            connectorPowerKw = req.connectorPowerKw,
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
    run("pricing.priceConsumption")(
      handler
        .priceConsumption(
          PriceConsumptionRequestADT(
            tenantId = req.tenantId,
            transactionId = req.transactionId,
            pricingModelJson = req.pricingModelJson,
            consumptionWh = req.consumptionWh,
            cumulatedWh = 0.0,
            totalDurationSecs = 0,
            inactivitySecs = 0,
            totalInactivitySecs = 0,
            intervalStart = req.intervalStart,
            intervalEnd = req.intervalEnd,
            timezone = "UTC",
            connectorType = req.connectorType,
            connectorPowerKw = if req.connectorPowerKw > 0 then req.connectorPowerKw else req.instantWatts / 1000.0,
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
    run("pricing.finalisePrice")(
      handler
        .finalisePrice(
          FinalisePriceRequestADT(
            tenantId = req.tenantId,
            transactionId = req.transactionId,
            pricingModelJson = req.pricingModelJson,
            totalConsumptionWh = req.totalConsumptionWh,
            startTimestamp = req.startTimestamp,
            endTimestamp = req.endTimestamp,
            timezone = "UTC",
            connectorType = req.connectorType,
            connectorPowerKw = req.connectorPowerKw
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

  val start: RIO[PricingGrpcHandler & PricingConfig & EvTracing, Unit] =
    for
      handler <- ZIO.service[PricingGrpcHandler]
      cfg     <- ZIO.service[PricingConfig]
      tracing <- ZIO.service[EvTracing]
      rt      <- ZIO.runtime[Any]
      impl = new PricingGrpcTransport(handler, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(PricingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Pricing gRPC server listening on :${server.getPort}")
    yield ()
