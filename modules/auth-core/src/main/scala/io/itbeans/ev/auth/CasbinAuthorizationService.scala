package io.itbeans.ev.auth

import io.itbeans.ev.domain.UserRole
import org.casbin.jcasbin.main.Enforcer
import zio._

// ---------------------------------------------------------------------------
// Casbin-backed RBAC + custom ZIO ABAC layer.
//
// RBAC policy is loaded from casbin/rbac_policy.csv.
// ABAC dynamic filters mirror the TypeScript dynamic-filters/ directory —
// each filter is a ZIO function that transforms a MongoDB query based on
// the UserToken's ABAC context (sites, sitesAdmin, sitesOwner).
// ---------------------------------------------------------------------------

final class CasbinAuthorizationService(enforcer: Enforcer) extends AuthorizationService:

  override def authorize(req: AuthRequest): UIO[AuthResult] =
    ZIO.succeed {
      val subject  = req.token.role.toString
      val resource = req.resource.toString
      val action   = req.action.toString
      if enforcer.enforce(subject, resource, action)
      then Authorized
      else Denied(s"Role '${req.token.role}' is not allowed to '$action' on '$resource'")
    }

  override def buildFilter(req: AuthRequest): UIO[Option[AuthFilter]] =
    ZIO.succeed {
      req.token.role match
        case UserRole.SuperAdmin | UserRole.Admin =>
          // Tenant scoping is handled transparently by the mongo-zio collection accessor
          None

        // SiteAdmin sees documents belonging to the sites they administer
        case UserRole.SiteAdmin =>
          filterBySiteRole(req.resource, req.token.sitesAdmin)

        // SiteOwner sees documents belonging to the sites they own
        case UserRole.SiteOwner =>
          filterBySiteRole(req.resource, req.token.sitesOwner)

        // Basic / Demo: own-user resources filtered by userId; site-bound resources
        // filtered by the union of all assigned sites
        case UserRole.Basic | UserRole.Demo =>
          filterForOwnUser(req.resource, req.token)
    }

  // ── Private filter builders ───────────────────────────────────────────────

  /** Deny-all sentinel: matches nothing in any collection. */
  private val denyAll: Option[AuthFilter] =
    Some(AuthFilter(Map("_id" -> Map("$exists" -> false))))

  /** `{ siteID: { $in: ids } }` — for resources that carry a siteID field. */
  private def siteIdIn(ids: List[String]): Option[AuthFilter] =
    if ids.isEmpty then denyAll
    else Some(AuthFilter(Map("siteID" -> Map("$in" -> ids))))

  /** `{ _id: { $in: ids } }` — for the Site document itself (its _id IS the siteId). */
  private def docIdIn(ids: List[String]): Option[AuthFilter] =
    if ids.isEmpty then denyAll
    else Some(AuthFilter(Map("_id" -> Map("$in" -> ids))))

  /** `{ userID: { $eq: uid } }` — for resources the user owns directly. */
  private def userIdEq(uid: String): Option[AuthFilter] =
    Some(AuthFilter(Map("userID" -> Map("$eq" -> uid))))

  /** `{ _id: { $eq: uid } }` — for the User document (its _id is the userId). */
  private def docIdEq(uid: String): Option[AuthFilter] =
    Some(AuthFilter(Map("_id" -> Map("$eq" -> uid))))

  /**
   * SiteAdmin / SiteOwner role filter.
   * Resources that ARE a site use `_id`; everything else uses `siteID`.
   * Public reference data (CarCatalog, Setting) needs no filter.
   */
  private def filterBySiteRole(resource: Resource, siteIds: List[String]): Option[AuthFilter] =
    resource match
      case Resource.CarCatalog | Resource.Setting => None
      case Resource.Site                          => docIdIn(siteIds)
      case _                                      => siteIdIn(siteIds)

  /**
   * Basic / Demo role filter — dispatches by resource.
   *
   * OwnUser resources:   filter by userId (or _id for User document).
   * Site-bound resources: filter by all sites the user belongs to.
   * Company:             filter by the user's associated company IDs.
   * Public data:         no filter.
   */
  private def filterForOwnUser(resource: Resource, token: UserToken): Option[AuthFilter] =
    val uid      = token.id.value
    val allSites = (token.sites ++ token.sitesAdmin ++ token.sitesOwner).distinct
    resource match
      // The User document uses _id, not a userID field
      case Resource.User =>
        docIdEq(uid)
      // Resources owned directly by the user
      case Resource.Car | Resource.Invoice | Resource.PaymentMethod |
          Resource.Tag | Resource.Notification | Resource.BillingTransfer |
          Resource.Transaction =>
        userIdEq(uid)
      // Companies: filter by the company IDs embedded in the token
      // (populated at login from the sites the user belongs to)
      case Resource.Company =>
        if token.companyIDs.isEmpty then denyAll
        else docIdIn(token.companyIDs)
      // The Site document uses _id
      case Resource.Site =>
        docIdIn(allSites)
      // Public reference data needs no filter
      case Resource.CarCatalog | Resource.Setting =>
        None
      // All other site-bound resources (SiteArea, ChargingStation, Asset, etc.)
      case _ =>
        siteIdIn(allSites)

  override def projectFields(resource: Resource, token: UserToken): UIO[Set[String]] =
    ZIO.succeed(hiddenFields(resource, token.role))

  // Fields removed from API responses for each (resource, role) combination.
  // Returns the set of field names that must be stripped before sending to the client.
  // Mirrors TypeScript addSiteAdmin16ProjectFields / addBasicProjectFields patterns.
  private def hiddenFields(resource: Resource, role: UserRole): Set[String] =
    import UserRole._
    role match
      case SuperAdmin | Admin =>
        // Admins see everything except the password hash (never exposed over the wire)
        resourceAlwaysHidden(resource)

      case SiteAdmin | SiteOwner =>
        resourceAlwaysHidden(resource) ++ (resource match
          case Resource.User =>
            // SiteAdmin cannot see billing internals or security fields of other users
            Set(
              "billingData.customerID",
              "billingData.cardFingerprint",
              "billingData.paymentIntentID",
              "billingData.subscriptionID",
              "mobileToken",
              "mobileLastChangedDate"
            )
          case Resource.ChargingStation =>
            // SiteAdmin cannot see OCPP credentials
            Set("registrationStatus", "chargeBoxSerialNumber")
          case _ => Set.empty
        )

      case Basic | Demo =>
        resourceAlwaysHidden(resource) ++ (resource match
          case Resource.User =>
            // Basic users cannot see other users' private details
            Set(
              "email",
              "phone",
              "mobile",
              "address",
              "billingData",
              "mobileToken",
              "mobileLastChangedDate",
              "lastLoginOn",
              "notificationsActive"
            )
          case Resource.ChargingStation =>
            Set(
              "registrationStatus",
              "chargeBoxSerialNumber",
              "chargeBoxID",
              "endpoint",
              "ocppVersion",
              "currentIPAddress"
            )
          case Resource.Transaction =>
            // Basic users see their own transactions but not pricing internals
            Set("priceUnit", "roundedPrice", "billingData")
          case _ => Set.empty
        )

  // Fields that are ALWAYS hidden regardless of role (e.g. hashed passwords)
  private def resourceAlwaysHidden(resource: Resource): Set[String] =
    resource match
      case Resource.User => Set("passwordHash")
      case _             => Set.empty

object CasbinAuthorizationService:

  val live: ZLayer[Any, Throwable, AuthorizationService] =
    ZLayer.scoped {
      ZIO.attempt {
        // Model and policy files are bundled in resources
        val modelPath  = getClass.getResource("/casbin/rbac_model.conf").getPath
        val policyPath = getClass.getResource("/casbin/rbac_policy.csv").getPath
        val enforcer   = new Enforcer(modelPath, policyPath)
        new CasbinAuthorizationService(enforcer)
      }
    }
