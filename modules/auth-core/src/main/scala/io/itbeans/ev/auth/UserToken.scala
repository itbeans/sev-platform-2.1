package io.itbeans.ev.auth

import io.itbeans.ev.domain.{TenantId, UserId, UserRole}
import zio.schema.{DeriveSchema, Schema}

// ---------------------------------------------------------------------------
// UserToken — the fat JWT payload mirroring the TypeScript UserToken interface.
// ALL fields must be preserved exactly (same camelCase names) for backward
// compatibility with existing dashboard clients and charging station tokens.
// ---------------------------------------------------------------------------

case class UserToken(
    // Identity
    id: UserId,
    tenantID: TenantId,
    tenantName: String,
    tenantSubdomain: String,
    email: String,
    name: String,
    firstName: String,
    lastName: String,
    // Authorization
    role: UserRole,
    rolesACL: RolesACL,
    // ABAC context (site-level ownership/admin/access lists)
    sites: List[String],
    sitesAdmin: List[String],
    sitesOwner: List[String],
    // Company IDs derived from assigned sites — used for the AssignedSitesCompanies ABAC filter
    companyIDs: List[String] = Nil,
    // Active components for this tenant
    activeComponents: List[String],
    // Token metadata
    locale: String,
    currency: Option[String],
    // Mobile / notification
    notificationsActive: Boolean,
    mobileOs: Option[String],
    // Technical token flag
    technical: Boolean,
    // Standard JWT claims
    issuer: String,
    // Computed at login, validated by Auth Service
    scopes: List[String]
)

/** Serialised RBAC grants carried in the token — mirrors role-acl output. */
case class RolesACL(grants: Map[String, List[String]])

object UserToken:
  given Schema[RolesACL] = DeriveSchema.gen
  given Schema[UserToken] = DeriveSchema.gen
