package io.itbeans.ev.authservice

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._

import java.time.Instant

// Circe instances for types without built-in support.
private[authservice] object CirceInstances:
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  given Decoder[Instant] = Decoder.decodeString.emap { s =>
    scala.util.Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  }

import CirceInstances.given

// ---------------------------------------------------------------------------
// AuthDomain — domain types for ev-auth-service.
//
// CRITICAL: UserToken field names MUST match the TypeScript UserToken interface
// exactly (including acronym capitalisation: tenantID, tagIDs, userHashID, etc.)
// for JWT compatibility with the dashboard frontend.
// ---------------------------------------------------------------------------

/**
 * JWT payload — byte-for-byte compatible with the TypeScript UserToken interface.
 *
 * All fields except `tenantID` are optional, mirroring the TypeScript definition.
 * The `user` field carries the embedded AuthUser JSON so the frontend can
 * display user info without making additional API calls.
 */
case class UserToken(
    tenantID: String, // MANDATORY
    id: Option[String] = None,
    role: Option[String] = None,
    rolesACL: Option[List[String]] = None,
    name: Option[String] = None,
    email: Option[String] = None,
    mobile: Option[String] = None,
    firstName: Option[String] = None,
    locale: Option[String] = None,
    language: Option[String] = None,
    currency: Option[String] = None,
    tagIDs: Option[List[String]] = None,
    tenantSubdomain: Option[String] = None,
    tenantName: Option[String] = None,
    userHashID: Option[String] = None,
    tenantHashID: Option[String] = None,
    scopes: Option[List[String]] = None,
    companies: Option[List[String]] = None,
    sites: Option[List[String]] = None,
    sitesAdmin: Option[List[String]] = None,
    sitesOwner: Option[List[String]] = None,
    activeComponents: Option[List[String]] = None,
    user: Option[Json] = None, // embedded user object for frontend
    technical: Option[Boolean] = None
)

object UserToken:
  given Encoder[UserToken] = deriveEncoder[UserToken].mapJson(_.dropNullValues)
  given Decoder[UserToken] = deriveDecoder

// ── Auth-specific user status ─────────────────────────────────────────────

/** User account status (extends domain UserStatus with Locked state). */
enum AuthUserStatus(val code: String):
  case Active   extends AuthUserStatus("A")
  case Inactive extends AuthUserStatus("I")
  case Blocked  extends AuthUserStatus("B")
  case Locked   extends AuthUserStatus("L") // temporary lockout (wrong passwords)
  case Pending  extends AuthUserStatus("P") // email not yet verified

object AuthUserStatus:
  def fromCode(code: String): Option[AuthUserStatus] = values.find(_.code == code)
  given Encoder[AuthUserStatus] = Encoder.encodeString.contramap(_.code)
  given Decoder[AuthUserStatus] = Decoder.decodeString.emap(fromCode(_).toRight("Unknown AuthUserStatus code"))

// ── Auth user (MongoDB representation) ───────────────────────────────────

/**
 * Minimal user representation owned by the auth service.
 * Stored in `{tenantId}.users`.
 *
 * Auth-specific fields (passwordWrongNbrTrials, passwordBlockedUntil,
 * technical, issuer) are NOT part of the shared ev-domain User type
 * because other services don't need them.
 */
case class AuthUser(
    id: String,
    email: String,
    password: String, // BCrypt hash (or legacy SHA-256 hex)
    firstName: String,
    name: String, // last name / display name
    mobile: Option[String],
    locale: String, // e.g. "en_US"
    role: String,   // role code matching TypeScript: "A", "B", etc.
    status: AuthUserStatus,
    technical: Boolean, // true = B2B/API user (cannot log in via UI)
    issuer: Boolean,    // true = locally-managed user
    passwordWrongNbrTrials: Int,
    passwordBlockedUntil: Option[Instant],
    companyIDs: List[String],
    notificationsActive: Boolean
)

object AuthUser:
  given Encoder[AuthUser] = deriveEncoder
  given Decoder[AuthUser] = deriveDecoder

// ── Tenant (read from global `tenants` collection) ────────────────────────

case class AuthTenant(
    id: String,
    subdomain: String,
    name: String,
    activeComponents: List[String], // component name strings (e.g. "Asset", "Pricing")
    currency: Option[String]
)

object AuthTenant:
  given Encoder[AuthTenant] = deriveEncoder
  given Decoder[AuthTenant] = deriveDecoder

// ── Tag (read from `{tenantId}.tags`) ────────────────────────────────────

case class AuthTag(
    id: String, // RFID/badge ID
    userID: Option[String],
    active: Boolean,
    issuer: Boolean
)

// ── User-site assignment (`{tenantId}.userssites`) ────────────────────────

case class UserSite(
    userID: String,
    siteID: String,
    siteAdmin: Boolean,
    siteOwner: Boolean
)
