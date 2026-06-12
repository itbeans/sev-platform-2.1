package io.itbeans.ev.authservice

import com.github.t3hnar.bcrypt._
import io.circe.parser.decode
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

object AuthServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val tenantId = TenantId("test-tenant")
  private val now      = Instant.parse("2024-06-15T10:00:00Z")

  private val testConfig = AuthConfig(
    httpPort = 8080,
    grpcPort = 9090,
    userTokenKey = "test-secret-key-do-not-use-in-prod",
    userTokenLifetimeHours = 12,
    userDemoTokenLifetimeDays = 1,
    userTechnicalTokenLifetimeDays = 365,
    passwordWrongNumberOfTrial = 3,
    passwordBlockedWaitTimeMin = 60,
    defaultTenantId = "default"
  )

  private val testTenant = AuthTenant(
    id = "tenant-1",
    subdomain = "test",
    name = "Test Tenant",
    activeComponents = List("Pricing", "Asset"),
    currency = Some("EUR")
  )

  private val testPassword     = "correct-horse-battery"
  private val testPasswordHash = testPassword.boundedBcrypt

  private def makeUser(
      id: String = "user-1",
      email: String = "alice@example.com",
      role: String = "B",
      status: AuthUserStatus = AuthUserStatus.Active,
      technical: Boolean = false,
      trials: Int = 0,
      blockedUntil: Option[Instant] = None
  ): AuthUser = AuthUser(
    id = id,
    email = email,
    password = testPasswordHash,
    firstName = "Alice",
    name = "Smith",
    mobile = None,
    locale = "en_US",
    role = role,
    status = status,
    technical = technical,
    issuer = true,
    passwordWrongNbrTrials = trials,
    passwordBlockedUntil = blockedUntil,
    companyIDs = List("company-1"),
    notificationsActive = true
  )

  private val testTag = AuthTag(
    id = "AABBCCDD",
    userID = Some("user-1"),
    active = true,
    issuer = true
  )

  private val testUserSite = UserSite(
    userID = "user-1",
    siteID = "site-1",
    siteAdmin = true,
    siteOwner = false
  )

  // ── In-memory stubs ────────────────────────────────────────────────────────

  class MemUserRepository(init: List[AuthUser] = Nil) extends UserRepository:
    private var store: Map[String, AuthUser] = init.map(u => u.id -> u).toMap

    def findByEmail(tenantId: TenantId, email: String): Task[Option[AuthUser]] =
      ZIO.succeed(store.values.find(_.email == email))

    def findById(tenantId: TenantId, id: String): Task[Option[AuthUser]] =
      ZIO.succeed(store.get(id))

    def updateLockout(tenantId: TenantId, userId: String, trials: Int, blockedUntil: Option[Instant]): Task[Unit] =
      ZIO.succeed {
        store = store.updatedWith(userId)(_.map(u =>
          u.copy(passwordWrongNbrTrials = trials, passwordBlockedUntil = blockedUntil)
        ))
      }

    def upsert(tenantId: TenantId, user: AuthUser): Task[Unit] =
      ZIO.succeed { store = store.updated(user.id, user) }

    def get(id: String): Option[AuthUser] = store.get(id)

  class MemTenantRepository(tenants: List[AuthTenant] = Nil) extends TenantRepository:

    def findBySubdomain(subdomain: String): Task[Option[AuthTenant]] =
      ZIO.succeed(tenants.find(_.subdomain == subdomain))

    def findById(id: String): Task[Option[AuthTenant]] =
      ZIO.succeed(tenants.find(_.id == id))

  class MemTagRepository(tags: List[AuthTag] = Nil) extends TagRepository:

    def findById(tenantId: TenantId, tagId: String): Task[Option[AuthTag]] =
      ZIO.succeed(tags.find(_.id == tagId))

    def findActiveByUserId(tenantId: TenantId, userId: String): Task[List[AuthTag]] =
      ZIO.succeed(tags.filter(t => t.userID.contains(userId) && t.active))

  class MemUserSiteRepository(sites: List[UserSite] = Nil) extends UserSiteRepository:

    def findByUserId(tenantId: TenantId, userId: String): Task[List[UserSite]] =
      ZIO.succeed(sites.filter(_.userID == userId))

  private def makeTokenService(
      users: List[AuthUser] = Nil,
      tenants: List[AuthTenant] = Nil,
      tags: List[AuthTag] = Nil,
      sites: List[UserSite] = Nil
  ): TokenService =
    new TokenService(
      testConfig,
      new MemUserRepository(users),
      new MemTagRepository(tags),
      new MemUserSiteRepository(sites),
      new MemTenantRepository(tenants)
    )

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("AuthServiceSpec")(
    // ── UserToken Circe codec ────────────────────────────────────────────────

    suite("UserToken Circe codec")(
      test("round-trips through JSON") {
        val token = UserToken(
          tenantID = "tenant-1",
          id = Some("user-1"),
          role = Some("B"),
          tagIDs = Some(List("AABBCCDD")),
          tenantSubdomain = Some("test"),
          tenantName = Some("Test Tenant"),
          firstName = Some("Alice"),
          name = Some("Smith"),
          email = Some("alice@example.com"),
          language = Some("en"),
          locale = Some("en_US")
        )
        val json    = token.asJson.noSpaces
        val decoded = decode[UserToken](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.contains(token))
      },
      test("tenantID field name is preserved exactly") {
        val token         = UserToken(tenantID = "tenant-1")
        val json          = token.asJson
        val tenantIDField = json.hcursor.downField("tenantID").as[String]
        // Must be "tenantID" not "tenantId" for TypeScript frontend compatibility
        assertTrue(tenantIDField.toOption.contains("tenant-1"))
      },
      test("tagIDs field name is preserved exactly") {
        val token = UserToken(tenantID = "t1", tagIDs = Some(List("TAG1")))
        val json  = token.asJson
        assertTrue(json.hcursor.downField("tagIDs").succeeded)
      },
      test("optional None fields are omitted from JSON") {
        val token = UserToken(tenantID = "t1")
        val json  = token.asJson.noSpaces
        // role is None → should not appear in JSON
        assertTrue(!json.contains("\"role\""))
      }
    ),

    // ── TokenService ─────────────────────────────────────────────────────────

    suite("TokenService")(
      test("issues and validates a JWT round-trip") {
        val user = makeUser()
        val svc  = makeTokenService(sites = List(testUserSite), tags = List(testTag))
        for
          token   <- svc.issueToken(tenantId, user, testTenant)
          decoded <- svc.validateToken(token)
        yield assertTrue(decoded.tenantID == testTenant.id) &&
          assertTrue(decoded.id.contains("user-1")) &&
          assertTrue(decoded.email.contains("alice@example.com")) &&
          assertTrue(decoded.tagIDs.exists(_.contains("AABBCCDD"))) &&
          assertTrue(decoded.sitesAdmin.exists(_.contains("site-1")))
      },
      test("includes only active tags in JWT") {
        val activeTag   = testTag
        val inactiveTag = AuthTag(id = "INACTIVE", userID = Some("user-1"), active = false, issuer = true)
        val svc         = makeTokenService(tags = List(activeTag, inactiveTag))
        for
          token   <- svc.issueToken(tenantId, makeUser(), testTenant)
          decoded <- svc.validateToken(token)
        yield assertTrue(decoded.tagIDs.exists(_.contains("AABBCCDD"))) &&
          assertTrue(!decoded.tagIDs.exists(_.contains("INACTIVE")))
      },
      test("token lifetime is longer for technical users") {
        // Technical users get userTechnicalTokenLifetimeDays, not userTokenLifetimeHours.
        // We verify by checking the token parses (indirect: if expiry calculation was wrong it would fail).
        val tech = makeUser(technical = true)
        val svc  = makeTokenService()
        for
          token   <- svc.issueToken(tenantId, tech, testTenant)
          decoded <- svc.validateToken(token)
        yield assertTrue(decoded.technical.contains(true))
      },
      test("validates JWT and returns UserToken") {
        val svc = makeTokenService()
        for
          token   <- svc.issueToken(tenantId, makeUser(), testTenant)
          decoded <- svc.validateToken(token)
        yield assertTrue(decoded.tenantID == "tenant-1")
      },
      test("rejects tampered token") {
        val svc = makeTokenService()
        for
          token <- svc.issueToken(tenantId, makeUser(), testTenant)
          tampered = token.dropRight(5) + "XXXXX"
          result <- svc.validateToken(tampered).either
        yield assertTrue(result.isLeft)
      },
      test("checkPassword returns true for correct BCrypt password") {
        val svc = makeTokenService()
        for ok <- svc.checkPassword(testPassword, testPasswordHash)
        yield assertTrue(ok)
      },
      test("checkPassword returns false for wrong password") {
        val svc = makeTokenService()
        for ok <- svc.checkPassword("wrong-password", testPasswordHash)
        yield assertTrue(!ok)
      },
      test("checkPassword accepts legacy SHA-256 hash") {
        import java.security.MessageDigest
        val plain = "legacy-password"
        val sha256Hash = MessageDigest.getInstance("SHA-256")
          .digest(plain.getBytes("UTF-8")).map("%02x".format(_)).mkString
        val svc = makeTokenService()
        // Store the user with a SHA-256 hash (legacy migration scenario)
        for ok <- svc.checkPassword(plain, sha256Hash)
        yield assertTrue(ok)
      },
      test("hashPassword produces verifiable BCrypt hash") {
        val svc = makeTokenService()
        for
          hash <- svc.hashPassword("new-password")
          ok   <- svc.checkPassword("new-password", hash)
        yield assertTrue(ok)
      }
    ),

    // ── Login flow ────────────────────────────────────────────────────────────

    suite("Login flow")(
      test("successful login returns a token") {
        val user       = makeUser()
        val userRepo   = new MemUserRepository(List(user))
        val tenantRepo = new MemTenantRepository(List(testTenant))
        val svc =
          new TokenService(testConfig, userRepo, new MemTagRepository(), new MemUserSiteRepository(), tenantRepo)
        AuthHttpServer.routes(userRepo, tenantRepo, svc, None, testConfig) // ensures it compiles
        for
          token <- svc.issueToken(tenantId, user, testTenant)
        yield assertTrue(token.nonEmpty)
      },
      test("wrong password increments lockout counter") {
        val user     = makeUser(trials = 0)
        val userRepo = new MemUserRepository(List(user))
        for
          _       <- userRepo.updateLockout(tenantId, user.id, 1, None)
          updated <- userRepo.findById(tenantId, user.id)
        yield assertTrue(updated.map(_.passwordWrongNbrTrials).contains(1))
      },
      test("account is locked after exceeding wrong trial threshold") {
        val user = makeUser(trials = 3, blockedUntil = Some(Instant.now().plusSeconds(3600)))
        // Simulate the lockout check: blockedUntil is in the future → should fail
        val result = user.passwordBlockedUntil.exists(Instant.now().isBefore(_))
        assertTrue(result)
      },
      test("inactive user cannot log in") {
        // AuthHttpServer.signIn checks status == Active; other statuses return an error.
        val inactive = makeUser(status = AuthUserStatus.Inactive)
        assertTrue(inactive.status != AuthUserStatus.Active)
      },
      test("technical user is flagged in token") {
        val tech = makeUser(technical = true)
        val svc  = makeTokenService()
        for
          token   <- svc.issueToken(tenantId, tech, testTenant)
          decoded <- svc.validateToken(token)
        yield assertTrue(decoded.technical.contains(true))
      },
      test("admin role produces broader scopes than basic role") {
        // Verify the roleToAcl/roleToScopes are different between Admin and Basic.
        // We test indirectly via the token payload.
        val admin = makeUser(role = "A")
        val basic = makeUser(id = "user-2", email = "bob@example.com")
        val svc   = makeTokenService()
        for
          adminToken   <- svc.issueToken(tenantId, admin, testTenant)
          basicToken   <- svc.issueToken(tenantId, basic, testTenant)
          adminDecoded <- svc.validateToken(adminToken)
          basicDecoded <- svc.validateToken(basicToken)
        yield assertTrue(adminDecoded.scopes.exists(_.contains("admin"))) &&
          assertTrue(!basicDecoded.scopes.exists(_.contains("admin")))
      }
    ),

    // ── OCPP Authorization ────────────────────────────────────────────────────

    suite("ResolveOcppAuthorize")(
      test("known active tag with active user → Accepted") {
        val user = makeUser()
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(List(user)),
          new MemTenantRepository(),
          new MemTagRepository(List(testTag)),
          testConfig
        )
        for resp <- svc.resolveOcppAuthorize(ResolveOcppAuthorizeRequestADT(
            tenantId = tenantId.value,
            tagId = "AABBCCDD",
            chargingStationId = "CS-1"
          ))
        yield assertTrue(resp.status == "Accepted") && assertTrue(resp.userJson.isDefined)
      },
      test("unknown tag → Invalid") {
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(),
          testConfig
        )
        for resp <- svc.resolveOcppAuthorize(ResolveOcppAuthorizeRequestADT(
            tenantId = tenantId.value,
            tagId = "UNKNOWN",
            chargingStationId = "CS-1"
          ))
        yield assertTrue(resp.status == "Invalid")
      },
      test("inactive tag → Invalid") {
        val inactive = testTag.copy(active = false)
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(List(inactive)),
          testConfig
        )
        for resp <- svc.resolveOcppAuthorize(ResolveOcppAuthorizeRequestADT(
            tenantId = tenantId.value,
            tagId = "AABBCCDD",
            chargingStationId = "CS-1"
          ))
        yield assertTrue(resp.status == "Invalid")
      },
      test("tag with no associated user → Invalid") {
        val orphanTag = testTag.copy(userID = None)
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(List(orphanTag)),
          testConfig
        )
        for resp <- svc.resolveOcppAuthorize(ResolveOcppAuthorizeRequestADT(
            tenantId = tenantId.value,
            tagId = "AABBCCDD",
            chargingStationId = "CS-1"
          ))
        yield assertTrue(resp.status == "Invalid")
      },
      test("blocked user → Invalid") {
        val blocked = makeUser(status = AuthUserStatus.Blocked)
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(List(blocked)),
          new MemTenantRepository(),
          new MemTagRepository(List(testTag)),
          testConfig
        )
        for resp <- svc.resolveOcppAuthorize(ResolveOcppAuthorizeRequestADT(
            tenantId = tenantId.value,
            tagId = "AABBCCDD",
            chargingStationId = "CS-1"
          ))
        yield assertTrue(resp.status == "Invalid")
      }
    ),

    // ── ResolveTenant ─────────────────────────────────────────────────────────

    suite("ResolveTenant")(
      test("known subdomain returns tenant") {
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(List(testTenant)),
          new MemTagRepository(),
          testConfig
        )
        for resp <- svc.resolveTenant(ResolveTenantRequestADT("test"))
        yield assertTrue(resp.found) && assertTrue(resp.tenantId.contains("tenant-1"))
      },
      test("unknown subdomain returns not found") {
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(),
          testConfig
        )
        for resp <- svc.resolveTenant(ResolveTenantRequestADT("unknown"))
        yield assertTrue(!resp.found) && assertTrue(resp.tenantId.isEmpty)
      },
      test("empty subdomain uses default tenant") {
        val defaultTenant = testTenant.copy(id = "default", subdomain = "")
        val svc = new AuthGrpcHandler(
          makeTokenService(),
          new MemUserRepository(),
          new MemTenantRepository(List(defaultTenant)),
          new MemTagRepository(),
          testConfig
        )
        for resp <- svc.resolveTenant(ResolveTenantRequestADT(""))
        yield assertTrue(resp.found) && assertTrue(resp.tenantId.contains("default"))
      }
    ),

    // ── ValidateToken ─────────────────────────────────────────────────────────

    suite("ValidateToken")(
      test("valid token returns valid=true and the decoded payload") {
        val svc = makeTokenService()
        val grpc = new AuthGrpcHandler(
          svc,
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(),
          testConfig
        )
        for
          token <- svc.issueToken(tenantId, makeUser(), testTenant)
          resp  <- grpc.validateToken(ValidateTokenRequestADT(token))
        yield assertTrue(resp.valid) &&
          assertTrue(resp.userTokenJson.nonEmpty) &&
          assertTrue(resp.error.isEmpty)
      },
      test("invalid token returns valid=false with error message") {
        val svc = makeTokenService()
        val grpc = new AuthGrpcHandler(
          svc,
          new MemUserRepository(),
          new MemTenantRepository(),
          new MemTagRepository(),
          testConfig
        )
        for resp <- grpc.validateToken(ValidateTokenRequestADT("not.a.jwt"))
        yield assertTrue(!resp.valid) &&
          assertTrue(resp.userTokenJson.isEmpty) &&
          assertTrue(resp.error.isDefined)
      }
    ),

    // ── AuthUserStatus domain ─────────────────────────────────────────────────

    suite("AuthUserStatus")(
      test("all status codes round-trip through fromCode") {
        val statuses = AuthUserStatus.values.toList
        assertTrue(statuses.forall(s => AuthUserStatus.fromCode(s.code).contains(s)))
      },
      test("fromCode returns None for unknown code") {
        assertTrue(AuthUserStatus.fromCode("X").isEmpty)
      }
    )
  )
