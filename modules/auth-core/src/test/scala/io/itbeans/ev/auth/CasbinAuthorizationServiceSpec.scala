package io.itbeans.ev.auth

import io.itbeans.ev.domain.{TenantId, UserId, UserRole}
import zio._
import zio.test._
import zio.test.Assertion._

// ---------------------------------------------------------------------------
// CasbinAuthorizationServiceSpec — policy correctness matrix.
//
// Each suite asserts the full set of role × resource × action expectations
// derived from AuthorizationsDefinition.ts.  Tests are grouped as:
//   - allow tests  (must return Authorized)
//   - deny tests   (must return Denied)
//   - buildFilter  (resource-aware MongoDB query fragment per role)
// ---------------------------------------------------------------------------

object CasbinAuthorizationServiceSpec extends ZIOSpecDefault:

  // ── Fixture helpers ───────────────────────────────────────────────────────

  def token(
      role: UserRole,
      sites: List[String] = Nil,
      sitesAdmin: List[String] = Nil,
      sitesOwner: List[String] = Nil,
      companyIDs: List[String] = Nil
  ): UserToken = UserToken(
    id = UserId("user-1"),
    tenantID = TenantId("tenant-1"),
    tenantName = "Test Tenant",
    tenantSubdomain = "test",
    email = "user@test.com",
    name = "Test",
    firstName = "User",
    lastName = "Test",
    role = role,
    rolesACL = RolesACL(Map.empty),
    sites = sites,
    sitesAdmin = sitesAdmin,
    sitesOwner = sitesOwner,
    companyIDs = companyIDs,
    activeComponents = Nil,
    locale = "en_US",
    currency = None,
    notificationsActive = false,
    mobileOs = None,
    technical = false,
    issuer = "test",
    scopes = Nil
  )

  def req(role: UserRole, resource: Resource, action: Action): AuthRequest =
    AuthRequest(resource, action, token(role))

  // ── Spec ──────────────────────────────────────────────────────────────────

  override def spec = suite("CasbinAuthorizationServiceSpec")(
    // ════════════════════════════════════════════════════════════════════════
    // RBAC — allow / deny matrix
    // ════════════════════════════════════════════════════════════════════════

    suite("SuperAdmin — full access")(
      test("can act on any resource and action") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SuperAdmin, Resource.ChargingStation, Action.Read))
            r2 <- svc.authorize(req(UserRole.SuperAdmin, Resource.User, Action.Delete))
            r3 <- svc.authorize(req(UserRole.SuperAdmin, Resource.Tenant, Action.Create))
            r4 <- svc.authorize(req(UserRole.SuperAdmin, Resource.ChargingStationTemplate, Action.Update))
            r5 <- svc.authorize(req(UserRole.SuperAdmin, Resource.Statistic, Action.Export))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized) &&
            assertTrue(r5 == Authorized)
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),
    suite("Admin — tenant-wide access")(
      test("can perform CRUD on core resources") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Admin, Resource.User, Action.List))
            r2 <- svc.authorize(req(UserRole.Admin, Resource.Site, Action.Create))
            r3 <- svc.authorize(req(UserRole.Admin, Resource.ChargingStation, Action.Reset))
            r4 <- svc.authorize(req(UserRole.Admin, Resource.Transaction, Action.List))
            r5 <- svc.authorize(req(UserRole.Admin, Resource.Tag, Action.Delete))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized) &&
            assertTrue(r5 == Authorized)
        }.flatten
      },
      test("can use OCPP remote commands") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Admin, Resource.ChargingStation, Action.RemoteStart))
            r2 <- svc.authorize(req(UserRole.Admin, Resource.ChargingStation, Action.ClearCache))
            r3 <- svc.authorize(req(UserRole.Admin, Resource.ChargingStation, Action.GetDiagnostics))
            r4 <- svc.authorize(req(UserRole.Admin, Resource.ChargingStation, Action.SetChargingProfile))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can manage logging and statistics") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Admin, Resource.Logging, Action.List))
            r2 <- svc.authorize(req(UserRole.Admin, Resource.Statistic, Action.Read))
            r3 <- svc.authorize(req(UserRole.Admin, Resource.Report, Action.Read))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),
    suite("SiteAdmin — site management + inherited Basic")(
      test("can manage users within their site") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.User, Action.List))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.User, Action.Create))
            r3 <- svc.authorize(req(UserRole.SiteAdmin, Resource.User, Action.Update))
            r4 <- svc.authorize(req(UserRole.SiteAdmin, Resource.User, Action.Delete))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can use OCPP commands on charging stations") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStation, Action.Reset))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStation, Action.ClearCache))
            r3 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStation, Action.ChangeConfiguration))
            r4 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStation, Action.GetDiagnostics))
            r5 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStation, Action.SetChargingProfile))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized) &&
            assertTrue(r5 == Authorized)
        }.flatten
      },
      test("can manage site areas") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.SiteArea, Action.Create))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.SiteArea, Action.Update))
            r3 <- svc.authorize(req(UserRole.SiteAdmin, Resource.SiteArea, Action.Delete))
            r4 <- svc.authorize(req(UserRole.SiteAdmin, Resource.SiteArea, Action.AssignChargingStations))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can manage charging profiles") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingProfile, Action.Create))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingProfile, Action.Delete))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized)
        }.flatten
      },
      test("inherits Basic — can read own user and assigned sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.User, Action.Read))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.Site, Action.List))
            r3 <- svc.authorize(req(UserRole.SiteAdmin, Resource.CarCatalog, Action.List))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("cannot create or delete a Tenant") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.Tenant, Action.Create))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.Tenant, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot manage ChargingStationTemplate") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStationTemplate, Action.Create))
            r2 <- svc.authorize(req(UserRole.SiteAdmin, Resource.ChargingStationTemplate, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),
    suite("SiteOwner — read/report + inherited Basic")(
      test("can read users and view statistics for owned sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteOwner, Resource.User, Action.List))
            r2 <- svc.authorize(req(UserRole.SiteOwner, Resource.User, Action.Read))
            r3 <- svc.authorize(req(UserRole.SiteOwner, Resource.Statistic, Action.Read))
            r4 <- svc.authorize(req(UserRole.SiteOwner, Resource.Statistic, Action.Export))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can read and export transactions") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteOwner, Resource.Transaction, Action.Read))
            r2 <- svc.authorize(req(UserRole.SiteOwner, Resource.Transaction, Action.GetCompletedTransaction))
            r3 <- svc.authorize(req(UserRole.SiteOwner, Resource.Transaction, Action.ExportCompletedTransaction))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("inherits Basic — can read charging stations and sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteOwner, Resource.ChargingStation, Action.List))
            r2 <- svc.authorize(req(UserRole.SiteOwner, Resource.Site, Action.Read))
            r3 <- svc.authorize(req(UserRole.SiteOwner, Resource.CarCatalog, Action.Read))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("cannot Reset charging stations or create site areas") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteOwner, Resource.ChargingStation, Action.Reset))
            r2 <- svc.authorize(req(UserRole.SiteOwner, Resource.SiteArea, Action.Create))
            r3 <- svc.authorize(req(UserRole.SiteOwner, Resource.ChargingStation, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied]) &&
            assertTrue(r3.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot manage users (create/update/delete)") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.SiteOwner, Resource.User, Action.Create))
            r2 <- svc.authorize(req(UserRole.SiteOwner, Resource.User, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),
    suite("Basic — own data only")(
      test("can read own user profile and update it") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.User, Action.Read))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.User, Action.Update))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized)
        }.flatten
      },
      test("can list and read car catalog") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.CarCatalog, Action.List))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.CarCatalog, Action.Read))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized)
        }.flatten
      },
      test("can manage own cars") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Car, Action.List))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Car, Action.Create))
            r3 <- svc.authorize(req(UserRole.Basic, Resource.Car, Action.Delete))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("can start and stop transactions") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.ChargingStation, Action.RemoteStartTransaction))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.ChargingStation, Action.RemoteStopTransaction))
            r3 <- svc.authorize(req(UserRole.Basic, Resource.Connector, Action.StartTransaction))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("can read own invoices and payment methods") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Invoice, Action.List))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Invoice, Action.Download))
            r3 <- svc.authorize(req(UserRole.Basic, Resource.PaymentMethod, Action.Create))
            r4 <- svc.authorize(req(UserRole.Basic, Resource.PaymentMethod, Action.Delete))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can list own tags and assign them") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Tag, Action.List))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Tag, Action.Assign))
            r3 <- svc.authorize(req(UserRole.Basic, Resource.Tag, Action.Unassign))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("cannot Reset or ClearCache charging stations") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.ChargingStation, Action.Reset))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.ChargingStation, Action.ClearCache))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot list all users") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for result <- svc.authorize(req(UserRole.Basic, Resource.User, Action.List))
          yield assertTrue(result.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot create or delete Sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Site, Action.Create))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Site, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot create or delete Companies") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Company, Action.Create))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Company, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot manage Tenant") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Basic, Resource.Tenant, Action.Read))
            r2 <- svc.authorize(req(UserRole.Basic, Resource.Tenant, Action.Update))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),
    suite("Demo — read-only subset")(
      test("can read charging stations and sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.List))
            r2 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.Read))
            r3 <- svc.authorize(req(UserRole.Demo, Resource.Site, Action.List))
            r4 <- svc.authorize(req(UserRole.Demo, Resource.SiteArea, Action.Read))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized) && assertTrue(r4 == Authorized)
        }.flatten
      },
      test("can read own transactions and statistics") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Demo, Resource.Transaction, Action.List))
            r2 <- svc.authorize(req(UserRole.Demo, Resource.Statistic, Action.Read))
            r3 <- svc.authorize(req(UserRole.Demo, Resource.Statistic, Action.Export))
          yield assertTrue(r1 == Authorized) && assertTrue(r2 == Authorized) &&
            assertTrue(r3 == Authorized)
        }.flatten
      },
      test("cannot start or stop transactions") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.StartTransaction))
            r2 <- svc.authorize(req(UserRole.Demo, Resource.Connector, Action.StartTransaction))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot create or delete Cars") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Demo, Resource.Car, Action.Create))
            r2 <- svc.authorize(req(UserRole.Demo, Resource.Car, Action.Delete))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied])
        }.flatten
      },
      test("cannot reset or control charging stations") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          for
            r1 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.Reset))
            r2 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.ClearCache))
            r3 <- svc.authorize(req(UserRole.Demo, Resource.ChargingStation, Action.Update))
          yield assertTrue(r1.isInstanceOf[Denied]) && assertTrue(r2.isInstanceOf[Denied]) &&
            assertTrue(r3.isInstanceOf[Denied])
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live),

    // ════════════════════════════════════════════════════════════════════════
    // ABAC — resource-aware MongoDB filter fragments
    // ════════════════════════════════════════════════════════════════════════

    suite("buildFilter — resource-aware MongoDB fragments")(
      // ── SuperAdmin / Admin ──────────────────────────────────────────────

      test("SuperAdmin returns no filter for any resource") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SuperAdmin, sites = List("s1"))
          for
            f1 <- svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t))
            f2 <- svc.buildFilter(AuthRequest(Resource.User, Action.List, t))
          yield assertTrue(f1.isEmpty) && assertTrue(f2.isEmpty)
        }.flatten
      },
      test("Admin returns no filter for any resource") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Admin, sites = List("s1"))
          for
            f1 <- svc.buildFilter(AuthRequest(Resource.Transaction, Action.List, t))
            f2 <- svc.buildFilter(AuthRequest(Resource.Site, Action.List, t))
          yield assertTrue(f1.isEmpty) && assertTrue(f2.isEmpty)
        }.flatten
      },

      // ── SiteAdmin ──────────────────────────────────────────────────────

      test("SiteAdmin: site-bound resources use siteID in sitesAdmin") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteAdmin, sitesAdmin = List("site-a", "site-b"))
          def siteIds(f: Option[AuthFilter]) =
            f.get.mongoFilter("siteID").asInstanceOf[Map[String, Any]]("$in")
              .asInstanceOf[List[String]]
          for
            fCs <- svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t))
            fTx <- svc.buildFilter(AuthRequest(Resource.Transaction, Action.List, t))
            fSa <- svc.buildFilter(AuthRequest(Resource.SiteArea, Action.List, t))
          yield assertTrue(siteIds(fCs).toSet == Set("site-a", "site-b")) &&
            assertTrue(siteIds(fTx).contains("site-b")) &&
            assertTrue(siteIds(fSa).contains("site-a"))
        }.flatten
      },
      test("SiteAdmin: Site resource uses _id in sitesAdmin") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteAdmin, sitesAdmin = List("site-a"))
          svc.buildFilter(AuthRequest(Resource.Site, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("_id").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids == List("site-a"))
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("SiteAdmin: CarCatalog and Setting return no filter") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteAdmin, sitesAdmin = List("site-a"))
          for
            f1 <- svc.buildFilter(AuthRequest(Resource.CarCatalog, Action.List, t))
            f2 <- svc.buildFilter(AuthRequest(Resource.Setting, Action.Read, t))
          yield assertTrue(f1.isEmpty) && assertTrue(f2.isEmpty)
        }.flatten
      },
      test("SiteAdmin with empty sitesAdmin returns deny-all") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteAdmin, sitesAdmin = Nil)
          svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t)).map {
            case Some(AuthFilter(m)) => assertTrue(m.contains("_id"))
            case _                   => assertTrue(false)
          }
        }.flatten
      },

      // ── SiteOwner ──────────────────────────────────────────────────────

      test("SiteOwner: site-bound resources use siteID in sitesOwner") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteOwner, sitesOwner = List("site-x"))
          svc.buildFilter(AuthRequest(Resource.Transaction, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("siteID").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids == List("site-x"))
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("SiteOwner: Site resource uses _id in sitesOwner") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.SiteOwner, sitesOwner = List("site-y"))
          svc.buildFilter(AuthRequest(Resource.Site, Action.Read, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("_id").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids == List("site-y"))
            case _ => assertTrue(false)
          }
        }.flatten
      },

      // ── Basic / Demo — own-user resources ─────────────────────────────

      test("Basic: User resource filters by _id eq userId") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic, sites = List("s1"))
          svc.buildFilter(AuthRequest(Resource.User, Action.Read, t)).map {
            case Some(AuthFilter(m)) =>
              assertTrue(m("_id").asInstanceOf[Map[String, Any]]("$eq") == "user-1")
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("Basic: Car/Invoice/Tag/PaymentMethod/Transaction filter by userID eq userId") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic, sites = List("s1"))
          def uid(f: Option[AuthFilter]) =
            f.get.mongoFilter("userID").asInstanceOf[Map[String, Any]]("$eq").asInstanceOf[String]
          for
            fCar <- svc.buildFilter(AuthRequest(Resource.Car, Action.List, t))
            fInv <- svc.buildFilter(AuthRequest(Resource.Invoice, Action.List, t))
            fTag <- svc.buildFilter(AuthRequest(Resource.Tag, Action.List, t))
            fPm  <- svc.buildFilter(AuthRequest(Resource.PaymentMethod, Action.List, t))
            fTx  <- svc.buildFilter(AuthRequest(Resource.Transaction, Action.List, t))
          yield assertTrue(uid(fCar) == "user-1") && assertTrue(uid(fInv) == "user-1") &&
            assertTrue(uid(fTag) == "user-1") && assertTrue(uid(fPm) == "user-1") &&
            assertTrue(uid(fTx) == "user-1")
        }.flatten
      },
      test("Basic: Site resource filters by _id in all assigned sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic, sites = List("s1"), sitesAdmin = List("s2"))
          svc.buildFilter(AuthRequest(Resource.Site, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("_id").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids.contains("s1")) && assertTrue(ids.contains("s2"))
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("Basic: ChargingStation/SiteArea filter by siteID in all assigned sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic, sites = List("s1"), sitesOwner = List("s3"))
          def siteIds(f: Option[AuthFilter]) =
            f.get.mongoFilter("siteID").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
          for
            fCs <- svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t))
            fSa <- svc.buildFilter(AuthRequest(Resource.SiteArea, Action.List, t))
          yield assertTrue(siteIds(fCs).contains("s1")) &&
            assertTrue(siteIds(fCs).contains("s3")) &&
            assertTrue(siteIds(fSa).contains("s1"))
        }.flatten
      },
      test("Basic: Company filters by _id in token companyIDs") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic, companyIDs = List("co-1", "co-2"))
          svc.buildFilter(AuthRequest(Resource.Company, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("_id").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids == List("co-1", "co-2"))
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("Basic: Company with empty companyIDs returns deny-all") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic)
          svc.buildFilter(AuthRequest(Resource.Company, Action.List, t)).map {
            case Some(AuthFilter(m)) => assertTrue(m.contains("_id"))
            case _                   => assertTrue(false)
          }
        }.flatten
      },
      test("Basic: CarCatalog and Setting return no filter") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic)
          for
            f1 <- svc.buildFilter(AuthRequest(Resource.CarCatalog, Action.List, t))
            f2 <- svc.buildFilter(AuthRequest(Resource.Setting, Action.Read, t))
          yield assertTrue(f1.isEmpty) && assertTrue(f2.isEmpty)
        }.flatten
      },
      test("Basic with no sites returns deny-all for site-bound resources") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Basic)
          svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t)).map {
            case Some(AuthFilter(m)) => assertTrue(m.contains("_id"))
            case _                   => assertTrue(false)
          }
        }.flatten
      },
      test("Demo: own-user resources filter by userID") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Demo, sites = List("site-demo"))
          svc.buildFilter(AuthRequest(Resource.Transaction, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              assertTrue(m("userID").asInstanceOf[Map[String, Any]]("$eq") == "user-1")
            case _ => assertTrue(false)
          }
        }.flatten
      },
      test("Demo: site-bound resources filter by siteID in assigned sites") {
        ZIO.serviceWith[AuthorizationService] { svc =>
          val t = token(UserRole.Demo, sites = List("site-demo"))
          svc.buildFilter(AuthRequest(Resource.ChargingStation, Action.List, t)).map {
            case Some(AuthFilter(m)) =>
              val ids = m("siteID").asInstanceOf[Map[String, Any]]("$in").asInstanceOf[List[String]]
              assertTrue(ids == List("site-demo"))
            case _ => assertTrue(false)
          }
        }.flatten
      }
    ).provide(CasbinAuthorizationService.live)
  )
