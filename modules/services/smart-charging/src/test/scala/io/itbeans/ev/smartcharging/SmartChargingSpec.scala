package io.itbeans.ev.smartcharging

import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

// ---------------------------------------------------------------------------
// SmartChargingSpec — ZIO Test suite for ev-smart-charging.
//
// Uses in-memory stub implementations of SapSmartChargingClient and
// SmartChargingRepository to test the service logic in isolation.
// ---------------------------------------------------------------------------

object SmartChargingSpec extends ZIOSpecDefault:

  // ── Stub SAP client ───────────────────────────────────────────────────────

  class StubSapClient(plan: List[Double] = List(16.0, 16.0, 16.0)) extends SapSmartChargingClient:

    def optimize(request: OptimizerRequest): Task[OptimizerResult] =
      ZIO.succeed(OptimizerResult(
        cars = request.state.cars.map(car => car.copy(currentPlan = plan))
      ))

    def checkConnection: Task[Boolean] = ZIO.succeed(true)

  class FailingSapClient(msg: String) extends SapSmartChargingClient:

    def optimize(request: OptimizerRequest): Task[OptimizerResult] =
      ZIO.fail(new Exception(msg))

    def checkConnection: Task[Boolean] = ZIO.succeed(false)

  // ── Stub gateway client (no station delivery in unit tests) ──────────────

  object NoOpGatewayClient extends SmartChargingGatewayClient:

    def deliverChargingProfile(tenantId: String, profile: ChargingProfile): Task[Boolean] =
      ZIO.succeed(true)

  // ── In-memory repository ──────────────────────────────────────────────────

  class InMemorySmartChargingRepository(
      siteAreaData: Map[String, SiteAreaSC] = Map.empty,
      txData: Map[String, List[ActiveTransactionSC]] = Map.empty
  ) extends SmartChargingRepository:

    private var savedProfiles: List[ChargingProfile] = Nil

    def saveProfile(profile: ChargingProfile): Task[Unit] =
      ZIO.succeed { savedProfiles = profile :: savedProfiles }

    def findProfiles(tenantId: String, siteAreaId: String): Task[List[ChargingProfile]] =
      ZIO.succeed(savedProfiles.filter(p => p.tenantId == tenantId && p.siteAreaId.contains(siteAreaId)))

    def deleteProfilesForStation(tenantId: String, chargingStationId: String): Task[Int] =
      ZIO.succeed {
        val before = savedProfiles.size
        savedProfiles = savedProfiles.filterNot(p =>
          p.tenantId == tenantId && p.chargingStationId == chargingStationId
        )
        before - savedProfiles.size
      }

    def findSiteArea(tenantId: String, siteAreaId: String): Task[Option[SiteAreaSC]] =
      ZIO.succeed(siteAreaData.get(siteAreaId))

    def findActiveTransactions(tenantId: String, siteAreaId: String): Task[List[ActiveTransactionSC]] =
      ZIO.succeed(txData.getOrElse(siteAreaId, Nil))

    def profiles: List[ChargingProfile] = savedProfiles

  // ── Test fixtures ─────────────────────────────────────────────────────────

  val testConfig = SmartChargingConfig(
    httpPort = 8080,
    grpcPort = 9090,
    optimizerUrl = "http://localhost:9999/optimize",
    optimizerUser = "test",
    optimizerPassword = "test",
    debounceSecs = 5,
    periodicIntervalMins = 15,
    defaultVoltage = 230,
    defaultNumberPhases = 3,
    defaultTenantId = "test-tenant",
    gatewayGrpcHost = "localhost",
    gatewayGrpcPort = 9999
  )

  val testSiteArea = SiteAreaSC(
    id = "sa-001",
    tenantId = "test-tenant",
    siteId = Some("site-001"),
    name = "Test Site Area",
    maximumPower = 22000.0, // 22 kW grid fuse
    voltage = Some(230),
    numberOfPhases = Some(3),
    smartCharging = true
  )

  val testTransaction = ActiveTransactionSC(
    id = 42L,
    chargingStationId = "CS-001",
    connectorId = 1,
    stateOfCharge = Some(50),
    departureTime = None,
    targetSoC = Some(80),
    maxCurrentA = 32.0,
    numberOfPhases = 3
  )

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("SmartChargingSpec")(
    suite("LiveSmartChargingService")(
      test("triggerOptimization creates profiles for active transactions") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        val sap = new StubSapClient(plan = List(16.0, 12.0, 8.0))
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        for
          _ <- svc.triggerOptimization("test-tenant", "sa-001", "Reoptimize")
          profiles = repo.profiles
        yield assertTrue(profiles.nonEmpty) &&
          assertTrue(profiles.head.chargingStationId == "CS-001") &&
          assertTrue(profiles.head.connectorId == 1) &&
          assertTrue(profiles.head.tenantId == "test-tenant") &&
          assertTrue(profiles.head.siteAreaId.contains("sa-001")) &&
          assertTrue(profiles.head.profile.chargingProfilePurpose == "TxProfile") &&
          assertTrue(profiles.head.profile.chargingSchedule.chargingRateUnit == "W")
      },
      test("triggerOptimization fails gracefully when site area is not found") {
        val repo = new InMemorySmartChargingRepository() // no site areas
        val sap  = new StubSapClient()
        val svc  = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        svc.triggerOptimization("test-tenant", "nonexistent-sa", "Reoptimize")
          .fold(
            err => assertTrue(err.getMessage.contains("not found")),
            _ => assertTrue(false) // should have failed
          )
      },
      test("triggerOptimization produces correct Watt values from optimizer amperage") {
        // 16 A * 230 V * 3 phases = 11040 W
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        val sap = new StubSapClient(plan = List(16.0))
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        for
          _ <- svc.triggerOptimization("test-tenant", "sa-001", "Reoptimize")
          profiles = repo.profiles
          period   = profiles.head.profile.chargingSchedule.periods.head
        yield assertTrue(math.abs(period.limitWatts - 11040.0) < 0.01) &&
          assertTrue(period.startPeriod == 0)
      },
      test("triggerOptimization propagates optimizer error") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        val sap = new FailingSapClient("SAP optimizer timeout")
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        svc.triggerOptimization("test-tenant", "sa-001", "Reoptimize")
          .fold(
            err => assertTrue(err.getMessage.contains("SAP optimizer timeout")),
            _ => assertTrue(false)
          )
      },
      test("triggerOptimization with no active transactions sends empty car list") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map.empty // no active transactions
        )
        var capturedRequest: Option[OptimizerRequest] = None
        val sap = new SapSmartChargingClient:
          def optimize(req: OptimizerRequest): Task[OptimizerResult] =
            ZIO.succeed {
              capturedRequest = Some(req)
              OptimizerResult(cars = Nil)
            }
          def checkConnection: Task[Boolean] = ZIO.succeed(true)
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        for
          _ <- svc.triggerOptimization("test-tenant", "sa-001", "Reoptimize")
        yield assertTrue(capturedRequest.exists(_.state.cars.isEmpty)) &&
          assertTrue(capturedRequest.exists(_.event.eventType == "Reoptimize"))
      }
    ),
    suite("OptimizerRequest construction")(
      test("fuse amperage = maximumPower / (voltage * phases)") {
        // 22000 W / (230 V * 3 phases) = ~31.88 A
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map.empty
        )
        var capturedRequest: Option[OptimizerRequest] = None
        val sap = new SapSmartChargingClient:
          def optimize(req: OptimizerRequest): Task[OptimizerResult] =
            ZIO.succeed {
              capturedRequest = Some(req)
              OptimizerResult(cars = Nil)
            }
          def checkConnection: Task[Boolean] = ZIO.succeed(true)
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        for
          _ <- svc.triggerOptimization("test-tenant", "sa-001", "Reoptimize")
        yield
          val expectedFuse = 22000.0 / (230.0 * 3.0)
          assertTrue(
            capturedRequest.exists(r =>
              r.state.fuseTree.get("0").exists(f =>
                math.abs(f.fusePhase1 - expectedFuse) < 0.01
              )
            )
          )
      },
      test("car name follows {chargingStationId}~{connectorId} convention") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        var capturedRequest: Option[OptimizerRequest] = None
        val sap = new SapSmartChargingClient:
          def optimize(req: OptimizerRequest): Task[OptimizerResult] =
            ZIO.succeed {
              capturedRequest = Some(req)
              OptimizerResult(cars = req.state.cars.map(_.copy(currentPlan = List(16.0))))
            }
          def checkConnection: Task[Boolean] = ZIO.succeed(true)
        val svc = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)

        for
          _ <- svc.triggerOptimization("test-tenant", "sa-001", "CarArrival")
        yield assertTrue(
          capturedRequest.exists(r =>
            r.state.cars.headOption.exists(_.name == "CS-001~1")
          )
        )
      }
    ),
    suite("SmartChargingGrpcHandler")(
      test("triggerSmartCharging returns success on valid request") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        val sap     = new StubSapClient()
        val svc     = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)
        val handler = new LiveSmartChargingGrpcHandler(svc, sap)

        for
          resp <- handler.triggerSmartCharging(
            TriggerSmartChargingRequestADT("test-tenant", "sa-001")
          )
        yield assertTrue(resp.success)
      },
      test("buildChargingProfiles returns computed profiles without delivering them") {
        val repo = new InMemorySmartChargingRepository(
          siteAreaData = Map("sa-001" -> testSiteArea),
          txData = Map("sa-001" -> List(testTransaction))
        )
        val sap     = new StubSapClient()
        val svc     = new LiveSmartChargingService(repo, sap, NoOpGatewayClient, testConfig)
        val handler = new LiveSmartChargingGrpcHandler(svc, sap)

        for
          resp <- handler.buildChargingProfiles(
            BuildChargingProfilesRequestADT("test-tenant", "sa-001")
          )
        yield assertTrue(resp.profiles.nonEmpty) &&
          assertTrue(resp.profiles.head.chargingStationId == "CS-001") &&
          assertTrue(resp.profiles.head.chargingProfileJson.contains("TxProfile"))
      }
    )
  )
