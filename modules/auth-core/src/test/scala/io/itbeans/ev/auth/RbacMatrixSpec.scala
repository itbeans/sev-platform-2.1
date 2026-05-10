package io.itbeans.ev.auth

import io.itbeans.ev.domain.{TenantId, UserId, UserRole}
import io.itbeans.ev.domain.UserRole._
import zio._
import zio.test._
import zio.test.Assertion._

// ---------------------------------------------------------------------------
// RbacMatrixSpec — data-driven 6 × 23 × 4 = 552 entry regression matrix.
//
// Encodes the complete allow/deny table for (role, resource, action) derived
// from AuthorizationsDefinition.ts.  Any Casbin policy change that breaks
// an expected grant or introduces an unexpected privilege will fail here.
//
// Actions covered: List, Read, Create, Delete  (the core CRUD-4 axes)
// Resources:  23 key domain entities (see coreResources)
// Roles:      SuperAdmin, Admin, SiteAdmin, SiteOwner, Basic, Demo
// ---------------------------------------------------------------------------

object RbacMatrixSpec extends ZIOSpecDefault:

  // Aliases to avoid shadowing scala.collection.immutable.List
  private val aList   = Action.List
  private val aRead   = Action.Read
  private val aCreate = Action.Create
  private val aDelete = Action.Delete

  private type Row = (UserRole, Resource, Action, Boolean)

  // ── Matrix builder helpers ─────────────────────────────────────────────────

  private def row(role: UserRole, r: Resource, l: Boolean, rd: Boolean, c: Boolean, d: Boolean): Vector[Row] =
    Vector(
      (role, r, aList,   l),
      (role, r, aRead,   rd),
      (role, r, aCreate, c),
      (role, r, aDelete, d)
    )

  private def allAllow(role: UserRole, r: Resource)  = row(role, r, true,  true,  true,  true)
  private def allDeny(role: UserRole, r: Resource)   = row(role, r, false, false, false, false)
  private def readOnly(role: UserRole, r: Resource)  = row(role, r, true,  true,  false, false)

  // ── Full 552-entry policy matrix ───────────────────────────────────────────

  private val matrix: Vector[Row] = {
    import Resource._

    // ── SuperAdmin: unrestricted platform access ─────────────────────────────
    val superAdmin =
      Vector(ChargingStation, ChargingStationTemplate, Connector, User, Tenant,
             Site, SiteArea, Company, Transaction, Tag, Asset, ChargingProfile,
             Car, CarCatalog, Invoice, PaymentMethod, BillingAccount,
             Notification, Logging, Statistic, Report, Setting, OcpiEndpoint)
        .flatMap(allAllow(SuperAdmin, _))

    // ── Admin: tenant-wide access; Tenant resource is SuperAdmin-only ────────
    val admin =
      Vector(ChargingStation, ChargingStationTemplate, Connector, User,
             Site, SiteArea, Company, Transaction, Tag, Asset, ChargingProfile,
             Car, CarCatalog, Invoice, PaymentMethod, BillingAccount,
             Notification, Logging, Statistic, Report, Setting, OcpiEndpoint)
        .flatMap(allAllow(Admin, _)) ++
      allDeny(Admin, Tenant)

    // ── SiteAdmin: site-level management; no platform or OCPI admin ──────────
    val siteAdmin =
      Vector(ChargingStation, Connector, User, Site, SiteArea, Transaction, Tag,
             Asset, ChargingProfile, Car, CarCatalog, Invoice,
             Notification, Logging, Statistic, Report)
        .flatMap(allAllow(SiteAdmin, _)) ++
      readOnly(SiteAdmin, ChargingStationTemplate) ++
      readOnly(SiteAdmin, BillingAccount) ++
      readOnly(SiteAdmin, Setting) ++
      allDeny(SiteAdmin, Tenant) ++
      allDeny(SiteAdmin, Company) ++
      allDeny(SiteAdmin, PaymentMethod) ++
      allDeny(SiteAdmin, OcpiEndpoint)

    // ── SiteOwner: read-only access; no mutations on management resources ────
    val siteOwner =
      Vector(ChargingStation, ChargingStationTemplate, Connector, User,
             Site, SiteArea, Company, Transaction, Tag, Asset, ChargingProfile,
             Car, CarCatalog, Invoice, BillingAccount, Notification,
             Logging, Statistic, Report)
        .flatMap(readOnly(SiteOwner, _)) ++
      allDeny(SiteOwner, Tenant) ++
      allDeny(SiteOwner, PaymentMethod) ++
      allDeny(SiteOwner, Setting) ++
      allDeny(SiteOwner, OcpiEndpoint)

    // ── Basic: own data only; no tenant/site/admin resources ─────────────────
    val basic =
      Vector(Site, SiteArea, Connector, CarCatalog, Transaction)
        .flatMap(readOnly(Basic, _)) ++
      allAllow(Basic, Car) ++
      row(Basic, ChargingStation, true,  true,  false, false) ++ // List + Read (own sessions)
      row(Basic, User,            false, true,  false, false) ++ // Read own profile; cannot list all
      row(Basic, Tag,             true,  true,  false, false) ++ // List + Read own tags (Assign/Unassign via custom actions)
      row(Basic, Invoice,         true,  true,  false, false) ++ // List + Read own invoices
      row(Basic, PaymentMethod,   true,  true,  true,  true)  ++ // Full CRUD on own payment methods
      allDeny(Basic, Tenant) ++
      allDeny(Basic, ChargingStationTemplate) ++
      allDeny(Basic, Company) ++
      allDeny(Basic, Asset) ++
      allDeny(Basic, ChargingProfile) ++
      allDeny(Basic, BillingAccount) ++
      allDeny(Basic, Notification) ++
      allDeny(Basic, Logging) ++
      allDeny(Basic, Statistic) ++
      allDeny(Basic, Report) ++
      allDeny(Basic, Setting) ++
      allDeny(Basic, OcpiEndpoint)

    // ── Demo: read-only on public resources; everything else denied ───────────
    val demo =
      Vector(ChargingStation, Site, SiteArea, Connector, Transaction, Statistic, CarCatalog)
        .flatMap(readOnly(Demo, _)) ++
      allDeny(Demo, User) ++
      allDeny(Demo, Tenant) ++
      allDeny(Demo, ChargingStationTemplate) ++
      allDeny(Demo, Company) ++
      allDeny(Demo, Tag) ++
      allDeny(Demo, Asset) ++
      allDeny(Demo, ChargingProfile) ++
      allDeny(Demo, Car) ++
      allDeny(Demo, Invoice) ++
      allDeny(Demo, PaymentMethod) ++
      allDeny(Demo, BillingAccount) ++
      allDeny(Demo, Notification) ++
      allDeny(Demo, Logging) ++
      allDeny(Demo, Report) ++
      allDeny(Demo, Setting) ++
      allDeny(Demo, OcpiEndpoint)

    superAdmin ++ admin ++ siteAdmin ++ siteOwner ++ basic ++ demo
  }

  // ── Fixtures ───────────────────────────────────────────────────────────────

  private def token(role: UserRole): UserToken = UserToken(
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
    sites = Nil,
    sitesAdmin = Nil,
    sitesOwner = Nil,
    companyIDs = Nil,
    activeComponents = Nil,
    locale = "en_US",
    currency = None,
    notificationsActive = false,
    mobileOs = None,
    technical = false,
    issuer = "test",
    scopes = Nil
  )

  private def req(role: UserRole, resource: Resource, action: Action): AuthRequest =
    AuthRequest(resource, action, token(role))

  // ── Spec ───────────────────────────────────────────────────────────────────

  override def spec = suite("RbacMatrixSpec")(

    test(s"full 6-role × 23-resource × 4-action matrix (${matrix.size} entries)") {
      ZIO.serviceWith[AuthorizationService] { svc =>
        ZIO.foreach(matrix) { case (role, resource, action, expectedAllow) =>
          svc.authorize(req(role, resource, action)).map { result =>
            val allowed = result == Authorized
            if allowed == expectedAllow then None
            else Some(s"$role/$resource/$action expected=${if expectedAllow then "allow" else "deny"} got=$result")
          }
        }.map { results =>
          val failures = results.flatten.toList
          assert(failures)(isEmpty)
        }
      }.flatten
    }

  ).provide(CasbinAuthorizationService.live)
