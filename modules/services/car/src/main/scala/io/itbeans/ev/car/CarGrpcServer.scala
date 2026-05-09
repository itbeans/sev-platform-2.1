package io.itbeans.ev.car

import io.itbeans.ev.domain.{TenantId, UserId}
import zio._

// ---------------------------------------------------------------------------
// CarGrpcServer — ZIO HTTP-based gRPC implementation for CarService.
//
// Implements the two RPCs defined in car_service.proto:
//   1. GetUserDefaultCar — returns the default Car for a given user
//   2. GetCurrentSoC     — returns live battery % from a car connector
//
// Note: The generated ScalaPB stubs (via sbt-protoc) will be available after
// running `sbt proto/compile`. This file wires them into ZIO service layers.
// For now the logic is implemented as plain ZIO effects; the gRPC binding
// wraps these with the generated ZioGrpc trait.
//
// The car service exposes port 9090 (gRPC) and 8080 (HTTP REST + healthcheck).
// ---------------------------------------------------------------------------

/** Request / Response ADTs (mirrors generated proto messages until codegen runs) */
case class GetUserDefaultCarRequest(tenantId: TenantId, userId: UserId)

case class GetUserDefaultCarResponse(
    found: Boolean,
    carId: String = "",
    carCatalogId: Int = 0,
    vin: String = "",
    licensePlate: String = "",
    converterPowerWatts: Double = 0.0,
    converterAmperagePhase: Double = 0.0,
    converterNumberOfPhases: Int = 0
)

case class GetCurrentSoCRequest(tenantId: TenantId, carId: String)
case class GetCurrentSoCResponse(found: Boolean, soc: Int = 0)

/** Pure business-logic layer — decoupled from gRPC transport so it can be unit-tested */
final class CarGrpcHandler(service: CarService):

  def getUserDefaultCar(req: GetUserDefaultCarRequest): Task[GetUserDefaultCarResponse] =
    service.getDefaultCar(req.tenantId, req.userId).map {
      case None =>
        GetUserDefaultCarResponse(found = false)
      case Some(car) =>
        GetUserDefaultCarResponse(
          found = true,
          carId = car.id,
          carCatalogId = car.carCatalogId,
          vin = car.vin.getOrElse(""),
          licensePlate = car.licensePlate.getOrElse(""),
          converterPowerWatts = car.converter.map(_.powerWatts).getOrElse(0.0),
          converterAmperagePhase = car.converter.map(_.amperagePerPhase).getOrElse(0.0),
          converterNumberOfPhases = car.converter.map(_.numberOfPhases).getOrElse(0)
        )
    }

  def getCurrentSoC(req: GetCurrentSoCRequest): Task[GetCurrentSoCResponse] =
    service.getCurrentSoC(req.tenantId, req.carId).map {
      case None      => GetCurrentSoCResponse(found = false)
      case Some(soc) => GetCurrentSoCResponse(found = true, soc = soc)
    }

object CarGrpcHandler:

  val live: ZLayer[CarService, Nothing, CarGrpcHandler] =
    ZLayer.fromFunction(new CarGrpcHandler(_))
