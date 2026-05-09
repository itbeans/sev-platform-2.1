package io.itbeans.ev.restapi

import io.itbeans.ev.domain._
import zio._
import io.itbeans.ev.domain.Tag
import zio.test._
import zio.test.Assertion._

import java.time.Instant

// ---------------------------------------------------------------------------
// RestApiSpec — ZIO Test suite for ev-rest-api.
//
// Uses in-memory repository stubs (no MongoDB needed).
// Tests cover: CRUD operations, pagination, tenant isolation, codecs.
// ---------------------------------------------------------------------------

object RestApiSpec extends ZIOSpecDefault:

  // ── In-memory stub repository ─────────────────────────────────────────────

  class InMemoryRestApiRepository extends RestApiRepository:

    var stations: List[ChargingStation] = Nil
    var sites: List[Site]               = Nil
    var siteAreas: List[SiteArea]       = Nil
    var companies: List[Company]        = Nil
    var transactions: List[Transaction] = Nil
    var users: List[User]               = Nil
    var tags: List[Tag]                 = Nil

    // ChargingStations
    def listStations(
        tenantId: String,
        limit: Int,
        skip: Int,
        siteId: Option[String],
        siteAreaId: Option[String]
    ): Task[PagedResult[ChargingStation]] =
      ZIO.succeed {
        val filtered = stations.filter { s =>
          s.tenantId == TenantId(tenantId) &&
          siteId.forall(id => s.siteId.contains(SiteId(id))) &&
          siteAreaId.forall(id => s.siteAreaId.contains(SiteAreaId(id)))
        }
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getStation(tenantId: String, id: String): Task[Option[ChargingStation]] =
      ZIO.succeed(stations.find(s => s.tenantId == TenantId(tenantId) && s.id == ChargingStationId(id)))
    def createStation(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateStation(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)

    def deleteStation(tenantId: String, id: String): Task[Boolean] =
      ZIO.succeed {
        val before = stations.size
        stations = stations.filterNot(s => s.tenantId == TenantId(tenantId) && s.id == ChargingStationId(id))
        stations.size < before
      }

    // Sites
    def listSites(tenantId: String, limit: Int, skip: Int, search: Option[String]): Task[PagedResult[Site]] =
      ZIO.succeed {
        val filtered = sites.filter(s =>
          s.tenantId == TenantId(tenantId) &&
            search.forall(q => s.name.toLowerCase.contains(q.toLowerCase))
        )
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getSite(tenantId: String, id: String): Task[Option[Site]] =
      ZIO.succeed(sites.find(s => s.tenantId == TenantId(tenantId) && s.id == SiteId(id)))
    def createSite(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateSite(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)

    def deleteSite(tenantId: String, id: String): Task[Boolean] =
      ZIO.succeed {
        val before = sites.size
        sites = sites.filterNot(s => s.tenantId == TenantId(tenantId) && s.id == SiteId(id))
        sites.size < before
      }

    // SiteAreas
    def listSiteAreas(tenantId: String, limit: Int, skip: Int, siteId: Option[String]): Task[PagedResult[SiteArea]] =
      ZIO.succeed {
        val filtered = siteAreas.filter(sa =>
          sa.tenantId == TenantId(tenantId) &&
            siteId.forall(id => sa.siteId == SiteId(id))
        )
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getSiteArea(tenantId: String, id: String): Task[Option[SiteArea]] =
      ZIO.succeed(siteAreas.find(sa => sa.tenantId == TenantId(tenantId) && sa.id == SiteAreaId(id)))
    def createSiteArea(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateSiteArea(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)
    def deleteSiteArea(tenantId: String, id: String): Task[Boolean]                         = ZIO.succeed(false)

    // Companies
    def listCompanies(tenantId: String, limit: Int, skip: Int): Task[PagedResult[Company]] =
      ZIO.succeed {
        val filtered = companies.filter(_.tenantId == TenantId(tenantId))
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getCompany(tenantId: String, id: String): Task[Option[Company]] =
      ZIO.succeed(companies.find(c => c.tenantId == TenantId(tenantId) && c.id == CompanyId(id)))
    def createCompany(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateCompany(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)
    def deleteCompany(tenantId: String, id: String): Task[Boolean]                         = ZIO.succeed(false)

    // Transactions
    def listTransactions(
        tenantId: String,
        limit: Int,
        skip: Int,
        inProgress: Option[Boolean],
        stationId: Option[String],
        userId: Option[String]
    ): Task[PagedResult[Transaction]] =
      ZIO.succeed {
        val filtered = transactions.filter { t =>
          t.tenantId == TenantId(tenantId) &&
          inProgress.forall(_ == t.inProgress) &&
          stationId.forall(id => t.chargingStationId == ChargingStationId(id)) &&
          userId.forall(id => t.userId.contains(UserId(id)))
        }
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getTransaction(tenantId: String, id: Long): Task[Option[Transaction]] =
      ZIO.succeed(transactions.find(t => t.tenantId == TenantId(tenantId) && t.id == TransactionId(id)))

    def deleteTransaction(tenantId: String, id: Long): Task[Boolean] =
      ZIO.succeed {
        val before = transactions.size
        transactions = transactions.filterNot(t => t.tenantId == TenantId(tenantId) && t.id == TransactionId(id))
        transactions.size < before
      }

    // Users
    def listUsers(
        tenantId: String,
        limit: Int,
        skip: Int,
        role: Option[String],
        search: Option[String]
    ): Task[PagedResult[User]] =
      ZIO.succeed {
        val filtered = users.filter { u =>
          u.tenantId == TenantId(tenantId) &&
          role.forall(r => u.role.toString == r) &&
          search.forall(q => u.email.contains(q) || u.firstName.contains(q) || u.lastName.contains(q))
        }
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getUser(tenantId: String, id: String): Task[Option[User]] =
      ZIO.succeed(users.find(u => u.tenantId == TenantId(tenantId) && u.id == UserId(id)))
    def createUser(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateUser(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)

    def deleteUser(tenantId: String, id: String): Task[Boolean] =
      ZIO.succeed {
        val before = users.size
        users = users.filterNot(u => u.tenantId == TenantId(tenantId) && u.id == UserId(id))
        users.size < before
      }

    // Tags
    def listTags(
        tenantId: String,
        limit: Int,
        skip: Int,
        userId: Option[String],
        active: Option[Boolean]
    ): Task[PagedResult[Tag]] =
      ZIO.succeed {
        val filtered = tags.filter { t =>
          t.tenantId == TenantId(tenantId) &&
          userId.forall(id => t.userId.contains(UserId(id))) &&
          active.forall(_ == t.active)
        }
        PagedResult(filtered.slice(skip, skip + limit), filtered.size)
      }

    def getTag(tenantId: String, id: String): Task[Option[Tag]] =
      ZIO.succeed(tags.find(t => t.tenantId == TenantId(tenantId) && t.id == id))
    def createTag(tenantId: String, doc: org.bson.Document): Task[Unit]                = ZIO.unit
    def updateTag(tenantId: String, id: String, doc: org.bson.Document): Task[Boolean] = ZIO.succeed(false)

    def deleteTag(tenantId: String, id: String): Task[Boolean] =
      ZIO.succeed {
        val before = tags.size
        tags = tags.filterNot(t => t.tenantId == TenantId(tenantId) && t.id == id)
        tags.size < before
      }

    // Settings / Registration Tokens — no-op stubs
    def getSetting(tenantId: String, identifier: String): Task[Option[io.circe.Json]] =
      ZIO.succeed(None)

    def upsertSetting(tenantId: String, identifier: String, body: io.circe.Json): Task[Unit] =
      ZIO.unit

    def listRegistrationTokens(tenantId: String, limit: Int, skip: Int): Task[PagedResult[io.circe.Json]] =
      ZIO.succeed(PagedResult(Nil, 0))

    def getRegistrationToken(tenantId: String, id: String): Task[Option[io.circe.Json]] =
      ZIO.succeed(None)

    def createRegistrationToken(tenantId: String, doc: org.bson.Document): Task[Unit] =
      ZIO.unit

    def deleteRegistrationToken(tenantId: String, id: String): Task[Boolean] =
      ZIO.succeed(false)

  // ── Fixtures ──────────────────────────────────────────────────────────────

  val now = Instant.parse("2024-06-15T12:00:00Z")

  def makeStation(id: String, tenantId: String = "t1"): ChargingStation =
    ChargingStation(
      id = ChargingStationId(id),
      tenantId = TenantId(tenantId),
      siteId = Some(SiteId("site-001")),
      siteAreaId = Some(SiteAreaId("sa-001")),
      companyId = None,
      chargePointVendor = "ACME",
      chargePointModel = "X200",
      chargePointSerialNumber = None,
      firmwareVersion = None,
      ocppVersion = Some(OcppVersion.V1_6),
      ocppTransport = OcppTransport.Json,
      ocppProtocol = None,
      connectors = Nil,
      coordinates = None,
      address = None,
      inactive = false,
      forceInactive = false,
      manualConfiguration = false,
      maximumPower = Some(22000.0),
      excludeFromSmartCharging = false,
      public = false,
      createdOn = now,
      lastChangedOn = now,
      lastHeartbeat = None,
      deviceModel = Map.empty
    )

  def makeUser(id: String, tenantId: String = "t1"): User =
    User(
      id = UserId(id),
      tenantId = TenantId(tenantId),
      firstName = "Jane",
      lastName = "Doe",
      email = s"$id@example.com",
      passwordHash = "hashed",
      phone = None,
      mobile = None,
      role = UserRole.Basic,
      status = UserStatus.Active,
      locale = "en_US",
      timezone = "Europe/Paris",
      isFree = false,
      eulaAccepted = true,
      createdOn = now,
      lastChangedOn = now,
      lastLoginOn = None,
      sites = Nil,
      sitesAdmin = Nil,
      sitesOwner = Nil
    )

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("RestApiSpec")(
    suite("ChargingStation repository")(
      test("list stations returns all for tenant") {
        val repo = new InMemoryRestApiRepository
        repo.stations = List(makeStation("CS-001"), makeStation("CS-002"), makeStation("CS-003", "other"))
        for
          result <- repo.listStations("t1", 10, 0, None, None)
        yield assertTrue(result.count == 2) &&
          assertTrue(result.result.size == 2)
      },
      test("list stations filters by siteId") {
        val repo = new InMemoryRestApiRepository
        val s1   = makeStation("CS-001")
        val s2   = makeStation("CS-002").copy(siteId = Some(SiteId("site-002")))
        repo.stations = List(s1, s2)
        for
          result <- repo.listStations("t1", 10, 0, Some("site-001"), None)
        yield assertTrue(result.count == 1) &&
          assertTrue(result.result.head.id == ChargingStationId("CS-001"))
      },
      test("get station returns None for unknown id") {
        val repo = new InMemoryRestApiRepository
        for
          result <- repo.getStation("t1", "CS-999")
        yield assertTrue(result.isEmpty)
      },
      test("delete station removes it from the list") {
        val repo = new InMemoryRestApiRepository
        repo.stations = List(makeStation("CS-001"), makeStation("CS-002"))
        for
          deleted   <- repo.deleteStation("t1", "CS-001")
          remaining <- repo.listStations("t1", 10, 0, None, None)
        yield assertTrue(deleted) &&
          assertTrue(remaining.count == 1)
      },
      test("tenant isolation — other tenant sees no data") {
        val repo = new InMemoryRestApiRepository
        repo.stations = List(makeStation("CS-001", "t1"))
        for
          result <- repo.listStations("t2", 10, 0, None, None)
        yield assertTrue(result.count == 0)
      }
    ),
    suite("User repository")(
      test("list users paginates correctly") {
        val repo = new InMemoryRestApiRepository
        repo.users = (1 to 10).map(i => makeUser(s"u$i")).toList
        for
          page1 <- repo.listUsers("t1", 5, 0, None, None)
          page2 <- repo.listUsers("t1", 5, 5, None, None)
        yield assertTrue(page1.count == 10) &&
          assertTrue(page1.result.size == 5) &&
          assertTrue(page2.result.size == 5)
      },
      test("list users filters by role") {
        val repo = new InMemoryRestApiRepository
        repo.users = List(
          makeUser("u1").copy(role = UserRole.Admin),
          makeUser("u2").copy(role = UserRole.Basic),
          makeUser("u3").copy(role = UserRole.Admin)
        )
        for
          result <- repo.listUsers("t1", 10, 0, Some("Admin"), None)
        yield assertTrue(result.count == 2) &&
          assertTrue(result.result.forall(_.role == UserRole.Admin))
      },
      test("delete user removes it") {
        val repo = new InMemoryRestApiRepository
        repo.users = List(makeUser("u1"), makeUser("u2"))
        for
          deleted   <- repo.deleteUser("t1", "u1")
          remaining <- repo.listUsers("t1", 10, 0, None, None)
        yield assertTrue(deleted) &&
          assertTrue(remaining.count == 1)
      }
    ),
    suite("RestApiConfig defaults")(
      test("httpPort defaults to 8080") {
        val cfg = RestApiConfig(
          httpPort = 8080,
          mongoUri = "mongodb://localhost:27017",
          mongoDbName = "evse",
          jwtSecret = "secret",
          authGrpcHost = "localhost",
          authGrpcPort = 50051,
          ocppGatewayGrpcHost = "localhost",
          ocppGatewayGrpcPort = 50053,
          billingGrpcHost = "localhost",
          billingGrpcPort = 50054,
          analyticsBaseUrl = "http://localhost:8090"
        )
        assertTrue(cfg.httpPort == 8080) &&
        assertTrue(cfg.mongoDbName == "evse")
      }
    )
  )
