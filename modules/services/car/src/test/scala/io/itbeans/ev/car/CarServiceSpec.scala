package io.itbeans.ev.car

import io.itbeans.ev.car.connector.CarConnectorRegistry
import io.itbeans.ev.domain.{TenantId, UserId}
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

object CarServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val tenantId: TenantId = TenantId("test-tenant")
  private val userId1: UserId    = UserId("user-1")
  private val userId2: UserId    = UserId("user-2")

  private val baseCatalog = CarCatalog(
    id = 1,
    vehicleMake = "Tesla",
    vehicleModel = "Model 3",
    vehicleModelVersion = Some("Long Range"),
    drivetrainPropulsion = Some("AWD"),
    drivetrainPowerHP = Some(450),
    performanceAcceleration = Some(4.4),
    performanceTopspeed = Some(225),
    rangeReal = Some(500),
    rangeWLTP = Some(560),
    efficiencyReal = Some(158.0),
    batteryCapacityUseable = Some(75.0),
    batteryCapacityFull = Some(82.0),
    chargePlug = Some("Type 2"),
    chargeStandardPower = Some(11.0),
    chargeStandardPhase = Some(3),
    chargeStandardPhaseAmp = Some(16.0),
    chargeStandardChargeTime = Some(480),
    chargeStandardChargeSpeed = Some(65.0),
    fastChargePlug = Some("CCS"),
    fastChargePowerMax = Some(250.0),
    miscBody = Some("Sedan"),
    miscSegment = Some("D"),
    miscSeats = Some(5),
    imageUrls = List("https://example.com/tesla-model3.jpg"),
    image = Some("data:image/jpeg;base64,/9j/..."),
    hash = Some("abc123"),
    imagesHash = Some("def456"),
    createdOn = Instant.parse("2024-01-01T00:00:00Z"),
    lastChangedOn = Instant.parse("2024-06-01T00:00:00Z")
  )

  private val baseCar = Car(
    id = "car-1",
    vin = Some("5YJ3E1EA1MF123456"),
    licensePlate = Some("AB-123-CD"),
    carCatalogId = 1,
    userId = userId1,
    carType = CarType.Private,
    default = true,
    converter = Some(CarConverter(11000.0, 16.0, 3, ConverterType.Standard)),
    carConnectorData = None,
    createdOn = Instant.now(),
    lastChangedOn = Instant.now()
  )

  // ── In-memory stubs ───────────────────────────────────────────────────────

  class MemCarCatalogRepository extends CarCatalogRepository:
    private var store: Map[Int, CarCatalog] = Map.empty

    def upsert(catalog: CarCatalog): Task[Unit] =
      ZIO.succeed { store = store.updated(catalog.id, catalog) }

    def findById(id: Int): Task[Option[CarCatalog]] =
      ZIO.succeed(store.get(id))

    def findAll(search: Option[String], makers: List[String], limit: Int, skip: Int): Task[List[CarCatalog]] =
      ZIO.succeed {
        store.values.toList
          .filter(c => makers.isEmpty || makers.contains(c.vehicleMake))
          .filter(c => search.forall(s => c.vehicleModel.toLowerCase.contains(s.toLowerCase)))
          .drop(skip)
          .take(limit)
      }

    def countAll(search: Option[String], makers: List[String]): Task[Long] =
      findAll(search, makers, Int.MaxValue, 0).map(_.length.toLong)

    def distinctMakers: Task[List[String]] =
      ZIO.succeed(store.values.map(_.vehicleMake).toList.distinct.sorted)

    def upsertImage(carId: Int, imageHash: String, imageBase64: String): Task[Unit] = ZIO.unit
    def getImage(imageHash: String): Task[Option[String]]                           = ZIO.none
    def deleteImages(carId: Int): Task[Unit]                                        = ZIO.unit

  class MemCarRepository extends CarRepository:
    private var store: Map[String, Car] = Map.empty

    def upsert(tenantId: TenantId, car: Car): Task[Unit] =
      ZIO.succeed { store = store.updated(car.id, car) }

    def findById(tenantId: TenantId, carId: String): Task[Option[Car]] =
      ZIO.succeed(store.get(carId))

    def findByUser(tenantId: TenantId, userId: UserId): Task[List[Car]] =
      ZIO.succeed(store.values.filter(_.userId == userId).toList)

    def findAll(
        tenantId: TenantId,
        userId: Option[UserId],
        search: Option[String],
        limit: Int,
        skip: Int
    ): Task[List[Car]] =
      ZIO.succeed {
        store.values.toList
          .filter(c => userId.forall(_ == c.userId))
          .filter(c => search.forall(s => c.licensePlate.exists(_.contains(s))))
          .drop(skip).take(limit)
      }

    def countAll(tenantId: TenantId, userId: Option[UserId], search: Option[String]): Task[Long] =
      findAll(tenantId, userId, search, Int.MaxValue, 0).map(_.length.toLong)

    def delete(tenantId: TenantId, carId: String): Task[Unit] =
      ZIO.succeed { store = store.removed(carId) }

    def clearDefault(tenantId: TenantId, userId: UserId): Task[Unit] =
      ZIO.succeed {
        store = store.view.mapValues { c =>
          if c.userId == userId then c.copy(default = false) else c
        }.toMap
      }

    def setDefault(tenantId: TenantId, carId: String): Task[Unit] =
      ZIO.succeed {
        store = store.view.mapValues { c =>
          if c.id == carId then c.copy(default = true) else c
        }.toMap
      }

    def findDefault(tenantId: TenantId, userId: UserId): Task[Option[Car]] =
      ZIO.succeed(store.values.find(c => c.userId == userId && c.default))

  class NoOpConnectorRegistry extends CarConnectorRegistry(Map.empty):
    override def getCurrentSoC(car: Car): Task[Option[Int]] = ZIO.none

  class MockEVDatabaseClient(catalogs: List[CarCatalog]) extends EVDatabaseClient:
    def fetchCatalogs: Task[List[CarCatalog]]                 = ZIO.succeed(catalogs)
    def fetchImageAsBase64(url: String): Task[Option[String]] = ZIO.succeed(Some("base64image"))

  def makeService(
      carRepo: CarRepository = new MemCarRepository,
      catalogRepo: CarCatalogRepository = new MemCarCatalogRepository,
      evClient: EVDatabaseClient = new MockEVDatabaseClient(List(baseCatalog))
  ): DefaultCarService =
    val syncTask = new SynchronizeCarsTask(evClient, catalogRepo)
    new DefaultCarService(carRepo, catalogRepo, syncTask, new NoOpConnectorRegistry)

  // ── Test cases ────────────────────────────────────────────────────────────

  override def spec = suite("ev-car")(
    suite("CarService — CRUD")(
      test("createCar stores car and auto-sets default when first car for user") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          car <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = Some("5YJ3E1EA1MF000001"),
              licensePlate = Some("AA-111-BB"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = false, // not requested as default
              converter = None,
              carConnectorData = None
            )
          )
          // Should auto-set as default since it's the only car for this user
          def1 <- carRepo.findDefault(tenantId, userId1)
        yield assertTrue(def1.isDefined) && assertTrue(def1.get.id == car.id)
      },
      test("createCar with default=true clears previous default") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          car1 <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("OLD-DEFAULT"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          car2 <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("NEW-DEFAULT"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          c1   <- carRepo.findById(tenantId, car1.id)
          def2 <- carRepo.findDefault(tenantId, userId1)
        yield assertTrue(!c1.get.default) && assertTrue(def2.get.id == car2.id)
      },
      test("deleteCar auto-assigns new default when deleting the default car") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          car1 <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("DEFAULT-CAR"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          car2 <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("SECOND-CAR"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = false,
              converter = None,
              carConnectorData = None
            )
          )
          _    <- svc.deleteCar(tenantId, car1.id)
          def2 <- carRepo.findDefault(tenantId, userId1)
        yield assertTrue(def2.isDefined)
      },
      test("getCars filters by userId") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          _ <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("U1-CAR"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          _ <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = Some("U2-CAR"),
              carCatalogId = 1,
              userId = userId2,
              carType = CarType.Company,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          u1Res <- svc.getCars(tenantId, Some(userId1), None, 100, 0)
          (u1Cars, u1Count) = u1Res
          u2Res <- svc.getCars(tenantId, Some(userId2), None, 100, 0)
          (u2Cars, u2Count) = u2Res
        yield assertTrue(u1Count == 1L) &&
          assertTrue(u1Cars.head.userId == userId1) &&
          assertTrue(u2Count == 1L) &&
          assertTrue(u2Cars.head.userId == userId2)
      },
      test("updateCar changes fields without affecting other cars") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          car <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = Some("OLD-VIN"),
              licensePlate = Some("OLD-PLATE"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          updated <- svc.updateCar(
            tenantId,
            car.id,
            UpdateCarRequest(
              vin = Some("NEW-VIN"),
              licensePlate = Some("NEW-PLATE"),
              carCatalogId = None,
              userId = None,
              carType = None,
              default = None,
              converter = None,
              carConnectorData = None
            )
          )
        yield assertTrue(updated.vin == Some("NEW-VIN")) &&
          assertTrue(updated.licensePlate == Some("NEW-PLATE")) &&
          assertTrue(updated.carType == CarType.Private)
      },
      test("createCar fails if catalog not found") {
        val svc = makeService()
        svc.createCar(
          tenantId,
          CreateCarRequest(
            vin = None,
            licensePlate = None,
            carCatalogId = 9999,
            userId = userId1,
            carType = CarType.Private,
            default = false,
            converter = None,
            carConnectorData = None
          )
        ).exit.map(e => assertTrue(e.isFailure))
      }
    ),
    suite("SynchronizeCarsTask")(
      test("syncs new catalogs from EVDatabase") {
        val catalogRepo = new MemCarCatalogRepository
        val evClient    = new MockEVDatabaseClient(List(baseCatalog))
        val syncTask    = new SynchronizeCarsTask(evClient, catalogRepo)
        for
          result <- syncTask.run
          stored <- catalogRepo.findById(baseCatalog.id)
        yield assertTrue(result.inSuccess == 1) &&
          assertTrue(result.inError == 0) &&
          assertTrue(stored.isDefined) &&
          assertTrue(stored.get.vehicleMake == "Tesla")
      },
      test("skips unchanged catalogs on second sync") {
        val catalogRepo = new MemCarCatalogRepository
        val evClient    = new MockEVDatabaseClient(List(baseCatalog))
        val syncTask    = new SynchronizeCarsTask(evClient, catalogRepo)
        for
          first  <- syncTask.run
          second <- syncTask.run
        yield assertTrue(first.inSuccess == 1) && assertTrue(second.inSuccess == 1)
        // Both should succeed (idempotent)
      },
      test("counts errors for each failed catalog") {
        val catalogRepo = new MemCarCatalogRepository
        // EVDatabase returns 2 catalogs, but the repository always fails on upsert
        val failingRepo = new MemCarCatalogRepository:
          override def upsert(catalog: CarCatalog): Task[Unit] =
            ZIO.fail(new Exception("DB down"))

        val cat2     = baseCatalog.copy(id = 2, vehicleModel = "Model S")
        val evClient = new MockEVDatabaseClient(List(baseCatalog, cat2))
        val syncTask = new SynchronizeCarsTask(evClient, failingRepo)
        for
          result <- syncTask.run
        yield assertTrue(result.inError == 2) && assertTrue(result.inSuccess == 0)
      }
    ),
    suite("CarCatalog queries")(
      test("getCatalogs returns all with no filter") {
        val catalogRepo = new MemCarCatalogRepository
        val svc         = makeService(catalogRepo = catalogRepo)
        for
          _           <- catalogRepo.upsert(baseCatalog)
          _           <- catalogRepo.upsert(baseCatalog.copy(id = 2, vehicleModel = "Model Y", vehicleMake = "Tesla"))
          _           <- catalogRepo.upsert(baseCatalog.copy(id = 3, vehicleModel = "e-tron", vehicleMake = "Audi"))
          catalogsRes <- svc.getCatalogs(None, Nil, 100, 0)
          (catalogs, total) = catalogsRes
        yield assertTrue(total == 3L) && assertTrue(catalogs.length == 3)
      },
      test("getCatalogs filters by maker") {
        val catalogRepo = new MemCarCatalogRepository
        val svc         = makeService(catalogRepo = catalogRepo)
        for
          _       <- catalogRepo.upsert(baseCatalog)
          _       <- catalogRepo.upsert(baseCatalog.copy(id = 2, vehicleModel = "Model Y", vehicleMake = "Tesla"))
          _       <- catalogRepo.upsert(baseCatalog.copy(id = 3, vehicleModel = "e-tron", vehicleMake = "Audi"))
          catsRes <- svc.getCatalogs(None, List("Audi"), 100, 0)
          (cats, total) = catsRes
        yield assertTrue(total == 1L) && assertTrue(cats.head.vehicleMake == "Audi")
      },
      test("getCarMakers returns distinct sorted list") {
        val catalogRepo = new MemCarCatalogRepository
        val svc         = makeService(catalogRepo = catalogRepo)
        for
          _      <- catalogRepo.upsert(baseCatalog)
          _      <- catalogRepo.upsert(baseCatalog.copy(id = 2, vehicleModel = "Model Y"))
          _      <- catalogRepo.upsert(baseCatalog.copy(id = 3, vehicleModel = "e-tron", vehicleMake = "Audi"))
          makers <- svc.getCarMakers
        yield assertTrue(makers == List("Audi", "Tesla"))
      }
    ),
    suite("CarDomain")(
      test("CarType encodes and decodes correctly") {
        import io.circe.syntax._
        import io.circe.parser.decode
        val encoded = CarType.PoolCar.asJson.noSpaces
        val decoded = decode[CarType](encoded)
        assertTrue(encoded == "\"PC\"") && assertTrue(decoded == Right(CarType.PoolCar))
      },
      test("ConverterType encodes and decodes correctly") {
        import io.circe.syntax._
        import io.circe.parser.decode
        val encoded = ConverterType.Alternative.asJson.noSpaces
        val decoded = decode[ConverterType](encoded)
        assertTrue(encoded == "\"A\"") && assertTrue(decoded == Right(ConverterType.Alternative))
      },
      test("CarCatalogSummary.from extracts key fields") {
        val summary = CarCatalogSummary.from(baseCatalog)
        assertTrue(summary.vehicleMake == "Tesla") &&
        assertTrue(summary.vehicleModel == "Model 3") &&
        assertTrue(summary.rangeReal == Some(500)) &&
        assertTrue(summary.batteryCapacity == Some(75.0)) &&
        assertTrue(summary.fastChargePowerMax == Some(250.0))
      }
    ),
    suite("gRPC handler")(
      test("GetUserDefaultCar returns found=true when user has a default car") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        val handler     = new CarGrpcHandler(svc)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          _ <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = Some("VIN001"),
              licensePlate = Some("GRPC-CAR"),
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = Some(CarConverter(11000.0, 16.0, 3, ConverterType.Standard)),
              carConnectorData = None
            )
          )
          resp <- handler.getUserDefaultCar(GetUserDefaultCarRequest(tenantId, userId1))
        yield assertTrue(resp.found) &&
          assertTrue(resp.converterPowerWatts == 11000.0) &&
          assertTrue(resp.converterNumberOfPhases == 3)
      },
      test("GetUserDefaultCar returns found=false for unknown user") {
        val svc     = makeService()
        val handler = new CarGrpcHandler(svc)
        for
          resp <- handler.getUserDefaultCar(GetUserDefaultCarRequest(tenantId, UserId("unknown-user")))
        yield assertTrue(!resp.found)
      },
      test("GetCurrentSoC returns found=false when no connector configured") {
        val catalogRepo = new MemCarCatalogRepository
        val carRepo     = new MemCarRepository
        val svc         = makeService(carRepo, catalogRepo)
        val handler     = new CarGrpcHandler(svc)
        for
          _ <- catalogRepo.upsert(baseCatalog)
          car <- svc.createCar(
            tenantId,
            CreateCarRequest(
              vin = None,
              licensePlate = None,
              carCatalogId = 1,
              userId = userId1,
              carType = CarType.Private,
              default = true,
              converter = None,
              carConnectorData = None
            )
          )
          resp <- handler.getCurrentSoC(GetCurrentSoCRequest(tenantId, car.id))
        yield assertTrue(!resp.found)
      }
    )
  )
