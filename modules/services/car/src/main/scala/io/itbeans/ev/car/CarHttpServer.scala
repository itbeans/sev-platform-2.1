package io.itbeans.ev.car

import io.itbeans.ev.domain.{TenantId, UserId}
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._
import zio.http.{Response, Routes}

// ---------------------------------------------------------------------------
// CarHttpServer — wires Tapir endpoints to ZIO HTTP routes.
//
// All handlers delegate to CarService. Errors are converted to HTTP 4xx/5xx
// by Tapir's ZIO HTTP interpreter (error strings → 400/500 by default).
// The server is started from Main by calling Server.serve(CarHttpServer.routes(svc)).
// ---------------------------------------------------------------------------

object CarHttpServer:

  def routes(svc: CarService): Routes[Any, Response] =
    val catalogServerEndpoints: List[ZServerEndpoint[Any, ZioStreams & WebSockets]] = List(
      CatalogEndpoints.listCatalogs.zServerLogic {
        case (tenantId, search, makerQ, limitQ, skipQ) =>
          val makers = makerQ.map(_.split(",").toList.map(_.trim)).getOrElse(Nil)
          svc.getCatalogs(search, makers, limitQ.getOrElse(100), skipQ.getOrElse(0))
            .map { case (cats, total) => CatalogListResponse(total, cats.map(CarCatalogSummary.from)) }
            .mapError(_.getMessage)
      },
      CatalogEndpoints.getCatalog.zServerLogic { case (id, _tenantId) =>
        svc.getCatalog(id)
          .flatMap {
            case Some(c) => ZIO.succeed(c)
            case None    => ZIO.fail(new Exception(s"CarCatalog $id not found"))
          }
          .mapError(_.getMessage)
      },
      CatalogEndpoints.getCatalogImage.zServerLogic { id =>
        svc.getCatalog(id)
          .map(c => ImageResponse(c.flatMap(_.image)))
          .mapError(_.getMessage)
      },
      CatalogEndpoints.getCarMakers.zServerLogic { _ =>
        svc.getCarMakers.map(MakersResponse(_)).mapError(_.getMessage)
      },
      CatalogEndpoints.syncCatalogs.zServerLogic { _ =>
        svc.syncCatalogs
          .map(r => SyncResponse(r.inSuccess, r.inError))
          .mapError(_.getMessage)
      }
    )

    val carServerEndpoints: List[ZServerEndpoint[Any, ZioStreams & WebSockets]] = List(
      CarEndpoints.listCars.zServerLogic {
        case (tenantId, userIdQ, search, limitQ, skipQ) =>
          svc.getCars(TenantId(tenantId), userIdQ.map(UserId(_)), search, limitQ.getOrElse(100), skipQ.getOrElse(0))
            .map { case (cars, total) => CarListResponse(total, cars) }
            .mapError(_.getMessage)
      },
      CarEndpoints.createCar.zServerLogic { case (tenantId, body) =>
        svc.createCar(
          TenantId(tenantId),
          CreateCarRequest(
            vin = body.vin,
            licensePlate = body.licensePlate,
            carCatalogId = body.carCatalogId,
            userId = UserId(body.userId),
            carType = body.carType,
            default = body.default,
            converter = body.converter,
            carConnectorData = body.carConnectorData
          )
        ).mapError(_.getMessage)
      },
      CarEndpoints.getCar.zServerLogic { case (id, tenantId) =>
        svc.getCar(TenantId(tenantId), id)
          .flatMap {
            case Some(c) => ZIO.succeed(c)
            case None    => ZIO.fail(new Exception(s"Car $id not found"))
          }
          .mapError(_.getMessage)
      },
      CarEndpoints.updateCar.zServerLogic { case (id, tenantId, body) =>
        svc.updateCar(
          TenantId(tenantId),
          id,
          UpdateCarRequest(
            vin = body.vin,
            licensePlate = body.licensePlate,
            carCatalogId = body.carCatalogId,
            userId = body.userId.map(UserId(_)),
            carType = body.carType,
            default = body.default,
            converter = body.converter,
            carConnectorData = body.carConnectorData
          )
        ).mapError(_.getMessage)
      },
      CarEndpoints.deleteCar.zServerLogic { case (id, tenantId) =>
        svc.deleteCar(TenantId(tenantId), id).mapError(_.getMessage)
      },
      CarEndpoints.getCarSoC.zServerLogic { case (id, tenantId) =>
        svc.getCurrentSoC(TenantId(tenantId), id)
          .map(socOpt => SoCResponse(found = socOpt.isDefined, soc = socOpt))
          .mapError(_.getMessage)
      }
    )

    ZioHttpInterpreter().toHttp(catalogServerEndpoints ++ carServerEndpoints)
