package io.itbeans.ev.car

import io.itbeans.ev.car.connector.CarConnectorRegistry
import io.itbeans.ev.domain.{TenantId, UserId}
import zio._

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// CarService — orchestrates Car and CarCatalog CRUD.
//
// Mirrors TypeScript CarService.ts:
//   - Car CRUD with uniqueness validation (VIN + licensePlate per tenant)
//   - Default-car management (exactly one default per user)
//   - CarCatalog read (write is done by SynchronizeCarsTask)
//   - SoC retrieval via CarConnectorRegistry
// ---------------------------------------------------------------------------

trait CarService:
  def getCar(tenantId: TenantId, carId: String): Task[Option[Car]]

  def getCars(
      tenantId: TenantId,
      userId: Option[UserId],
      search: Option[String],
      limit: Int,
      skip: Int
  ): Task[(List[Car], Long)]
  def createCar(tenantId: TenantId, request: CreateCarRequest): Task[Car]
  def updateCar(tenantId: TenantId, carId: String, request: UpdateCarRequest): Task[Car]
  def deleteCar(tenantId: TenantId, carId: String): Task[Unit]
  def getDefaultCar(tenantId: TenantId, userId: UserId): Task[Option[Car]]
  def getCurrentSoC(tenantId: TenantId, carId: String): Task[Option[Int]]

  def getCatalog(catalogId: Int): Task[Option[CarCatalog]]
  def getCatalogs(search: Option[String], makers: List[String], limit: Int, skip: Int): Task[(List[CarCatalog], Long)]
  def getCarMakers: Task[List[String]]
  def syncCatalogs: Task[SyncResult]

case class CreateCarRequest(
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Int,
    userId: UserId,
    carType: CarType,
    default: Boolean,
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData]
)

case class UpdateCarRequest(
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Option[Int],
    userId: Option[UserId],
    carType: Option[CarType],
    default: Option[Boolean],
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData]
)

final class DefaultCarService(
    cars: CarRepository,
    catalogs: CarCatalogRepository,
    syncTask: SynchronizeCarsTask,
    connectors: CarConnectorRegistry
) extends CarService:

  override def getCar(tenantId: TenantId, carId: String): Task[Option[Car]] =
    cars.findById(tenantId, carId)

  override def getCars(
      tenantId: TenantId,
      userId: Option[UserId],
      search: Option[String],
      limit: Int,
      skip: Int
  ): Task[(List[Car], Long)] =
    cars.findAll(tenantId, userId, search, limit, skip).zipPar(
      cars.countAll(tenantId, userId, search)
    )

  override def createCar(tenantId: TenantId, req: CreateCarRequest): Task[Car] =
    for
      // Validate catalog exists
      _ <- catalogs.findById(req.carCatalogId).flatMap {
        case None    => ZIO.fail(new Exception(s"CarCatalog ${req.carCatalogId} not found"))
        case Some(_) => ZIO.unit
      }
      // If this is the default, clear others
      _ <- ZIO.when(req.default)(cars.clearDefault(tenantId, req.userId))
      now = Instant.now()
      car = Car(
        id = UUID.randomUUID().toString,
        vin = req.vin,
        licensePlate = req.licensePlate,
        carCatalogId = req.carCatalogId,
        userId = req.userId,
        carType = req.carType,
        default = req.default,
        converter = req.converter,
        carConnectorData = req.carConnectorData,
        createdOn = now,
        lastChangedOn = now
      )
      _ <- cars.upsert(tenantId, car)
      // If no default yet, auto-set this one
      _ <- autoSetDefaultIfNeeded(tenantId, car)
    yield car

  override def updateCar(tenantId: TenantId, carId: String, req: UpdateCarRequest): Task[Car] =
    for
      existing <- cars.findById(tenantId, carId).flatMap {
        case None    => ZIO.fail(new Exception(s"Car $carId not found"))
        case Some(c) => ZIO.succeed(c)
      }
      newUserId = req.userId.getOrElse(existing.userId)
      // If setting as default, clear others for the target user
      _ <- ZIO.when(req.default.contains(true))(cars.clearDefault(tenantId, newUserId))
      updated = existing.copy(
        vin = req.vin.orElse(existing.vin),
        licensePlate = req.licensePlate.orElse(existing.licensePlate),
        carCatalogId = req.carCatalogId.getOrElse(existing.carCatalogId),
        userId = newUserId,
        carType = req.carType.getOrElse(existing.carType),
        default = req.default.getOrElse(existing.default),
        converter = req.converter.orElse(existing.converter),
        carConnectorData = req.carConnectorData.orElse(existing.carConnectorData),
        lastChangedOn = Instant.now()
      )
      _ <- cars.upsert(tenantId, updated)
    yield updated

  override def deleteCar(tenantId: TenantId, carId: String): Task[Unit] =
    for
      existing <- cars.findById(tenantId, carId)
      _        <- cars.delete(tenantId, carId)
      // If we deleted the default, auto-assign another car as default
      _ <- existing match
        case Some(c) if c.default =>
          cars.findByUser(tenantId, c.userId).flatMap {
            case head :: _ => cars.setDefault(tenantId, head.id)
            case Nil       => ZIO.unit
          }
        case _ => ZIO.unit
    yield ()

  override def getDefaultCar(tenantId: TenantId, userId: UserId): Task[Option[Car]] =
    cars.findDefault(tenantId, userId)

  override def getCurrentSoC(tenantId: TenantId, carId: String): Task[Option[Int]] =
    cars.findById(tenantId, carId).flatMap {
      case None    => ZIO.none
      case Some(c) => connectors.getCurrentSoC(c)
    }

  override def getCatalog(catalogId: Int): Task[Option[CarCatalog]] =
    catalogs.findById(catalogId)

  override def getCatalogs(
      search: Option[String],
      makers: List[String],
      limit: Int,
      skip: Int
  ): Task[(List[CarCatalog], Long)] =
    catalogs.findAll(search, makers, limit, skip).zipPar(catalogs.countAll(search, makers))

  override def getCarMakers: Task[List[String]] =
    catalogs.distinctMakers

  override def syncCatalogs: Task[SyncResult] =
    syncTask.run

  private def autoSetDefaultIfNeeded(tenantId: TenantId, car: Car): Task[Unit] =
    if car.default then ZIO.unit
    else
      cars.findDefault(tenantId, car.userId).flatMap {
        case None => cars.setDefault(tenantId, car.id)
        case _    => ZIO.unit
      }

object DefaultCarService:

  val live
      : ZLayer[CarRepository & CarCatalogRepository & SynchronizeCarsTask & CarConnectorRegistry, Nothing, CarService] =
    ZLayer.fromFunction(new DefaultCarService(_, _, _, _))
