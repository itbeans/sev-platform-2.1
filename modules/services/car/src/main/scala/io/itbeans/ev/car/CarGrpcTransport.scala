package io.itbeans.ev.car

import _root_.io.grpc.netty.NettyServerBuilder
import _root_.io.itbeans.ev.car.grpc.car_service.{
  CarServiceGrpc,
  GetCurrentSoCRequest => ProtoGetCurrentSoCRequest,
  GetCurrentSoCResponse => ProtoGetCurrentSoCResponse,
  GetUserDefaultCarRequest => ProtoGetUserDefaultCarRequest,
  GetUserDefaultCarResponse => ProtoGetUserDefaultCarResponse
}
import io.itbeans.ev.domain.{TenantId, UserId}
import io.itbeans.ev.otel.EvTracing
import zio._

import scala.concurrent.{ExecutionContext, Future}

final class CarGrpcTransport(handler: CarGrpcHandler, tracing: EvTracing, rt: Runtime[Any])
    extends CarServiceGrpc.CarService:

  private def run[A](spanName: String)(effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(tracing.spanTask(spanName)(effect)))

  override def getUserDefaultCar(req: ProtoGetUserDefaultCarRequest): Future[ProtoGetUserDefaultCarResponse] =
    run("car.getUserDefaultCar")(
      handler
        .getUserDefaultCar(
          GetUserDefaultCarRequest(TenantId(req.tenantId), UserId(req.userId))
        )
        .map { r =>
          ProtoGetUserDefaultCarResponse(
            found = r.found,
            carId = r.carId,
            carCatalogId = r.carCatalogId,
            vin = r.vin,
            licensePlate = r.licensePlate,
            converterPowerWatts = r.converterPowerWatts,
            converterAmperagePhase = r.converterAmperagePhase,
            converterNumberOfPhases = r.converterNumberOfPhases
          )
        }
    )

  override def getCurrentSoC(req: ProtoGetCurrentSoCRequest): Future[ProtoGetCurrentSoCResponse] =
    run("car.getCurrentSoC")(
      handler
        .getCurrentSoC(GetCurrentSoCRequest(TenantId(req.tenantId), req.carId))
        .map(r => ProtoGetCurrentSoCResponse(found = r.found, soc = r.soc))
    )

object CarGrpcTransport:

  val start: RIO[CarGrpcHandler & CarConfig & EvTracing, Unit] =
    for
      handler <- ZIO.service[CarGrpcHandler]
      cfg     <- ZIO.service[CarConfig]
      tracing <- ZIO.service[EvTracing]
      rt      <- ZIO.runtime[Any]
      impl = new CarGrpcTransport(handler, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(CarServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Car gRPC server listening on :${server.getPort}")
    yield ()
