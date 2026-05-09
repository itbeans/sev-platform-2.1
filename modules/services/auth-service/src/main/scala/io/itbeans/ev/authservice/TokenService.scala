package io.itbeans.ev.authservice

import com.github.t3hnar.bcrypt._
import io.circe.syntax._
import io.circe.parser.decode
import io.itbeans.ev.domain.TenantId
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import zio._

import java.security.MessageDigest
import java.time.Instant

// ---------------------------------------------------------------------------
// TokenService — JWT issuance/validation and BCrypt password operations.
//
// Three-tier token lifetime (mirrors TypeScript):
//   1. Technical users: userTechnicalTokenLifetimeDays days
//   2. Demo users:      userDemoTokenLifetimeDays days
//   3. Regular users:   userTokenLifetimeHours hours
//
// Hash IDs (userHashID, tenantHashID) change when the user's roles/sites or
// tenant's active components change, triggering client-side session expiry.
// ---------------------------------------------------------------------------

final class TokenService(
    cfg: AuthConfig,
    userRepo: UserRepository,
    tagRepo: TagRepository,
    siteRepo: UserSiteRepository,
    tenantRepo: TenantRepository
):

  // ── Token issuance ────────────────────────────────────────────────────────

  /**
   * Build and sign a JWT for the given user + tenant.
   * Fetches tags and sites from MongoDB to populate the full token payload.
   */
  def issueToken(tenantId: TenantId, user: AuthUser, tenant: AuthTenant): Task[String] =
    for
      tags  <- tagRepo.findActiveByUserId(tenantId, user.id)
      sites <- siteRepo.findByUserId(tenantId, user.id)
      token = buildUserToken(user, tenant, tags, sites)
      signed <- ZIO.attempt(signToken(token, user))
    yield signed

  /** Validate a Bearer JWT and decode its payload. */
  def validateToken(rawToken: String): Task[UserToken] =
    ZIO.fromTry(
      JwtCirce.decodeJson(rawToken, cfg.userTokenKey, Seq(JwtAlgorithm.HS256))
        .flatMap(json =>
          scala.util.Try(
            decode[UserToken](json.noSpaces) match
              case Left(e)  => throw new Exception(s"Malformed UserToken payload: ${e.getMessage}")
              case Right(t) => t
          )
        )
    )

  // ── Password operations ───────────────────────────────────────────────────

  /**
   * Verify a plaintext password against a stored hash.
   * Supports BCrypt (primary) and legacy SHA-256 hex (fallback).
   */
  def checkPassword(plain: String, storedHash: String): Task[Boolean] =
    ZIO.attempt {
      val bcryptOk =
        try plain.isBcryptedBounded(storedHash)
        catch case _: Exception => false
      bcryptOk || sha256hex(plain) == storedHash
    }

  def hashPassword(plain: String): Task[String] =
    ZIO.attempt(plain.boundedBcrypt)

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def buildUserToken(
      user: AuthUser,
      tenant: AuthTenant,
      tags: List[AuthTag],
      sites: List[UserSite]
  ): UserToken =
    val siteIds  = sites.map(_.siteID)
    val admSites = sites.filter(_.siteAdmin).map(_.siteID)
    val ownSites = sites.filter(_.siteOwner).map(_.siteID)
    val tagIds   = tags.filter(_.active).map(_.id)
    val lang     = user.locale.split("[-_]").headOption.getOrElse("en")
    UserToken(
      tenantID = tenant.id,
      id = Some(user.id),
      role = Some(user.role),
      rolesACL = Some(roleToAcl(user.role)),
      name = Some(user.name),
      email = Some(user.email),
      mobile = user.mobile,
      firstName = Some(user.firstName),
      locale = Some(user.locale),
      language = Some(lang),
      currency = tenant.currency,
      tagIDs = Some(tagIds),
      tenantSubdomain = Some(tenant.subdomain),
      tenantName = Some(tenant.name),
      userHashID = Some(userHashId(user)),
      tenantHashID = Some(tenantHashId(tenant)),
      scopes = Some(roleToScopes(user.role)),
      companies = Some(user.companyIDs),
      sites = Some(siteIds),
      sitesAdmin = Some(admSites),
      sitesOwner = Some(ownSites),
      activeComponents = Some(tenant.activeComponents),
      user = Some(user.asJson),
      technical = if user.technical then Some(true) else None
    )

  private def signToken(token: UserToken, user: AuthUser): String =
    val nowSecs      = Instant.now().getEpochSecond
    val lifetimeSecs = tokenLifetime(user)
    val claim = JwtClaim(
      content = token.asJson.noSpaces,
      issuedAt = Some(nowSecs),
      expiration = Some(nowSecs + lifetimeSecs)
    )
    JwtCirce.encode(claim, cfg.userTokenKey, JwtAlgorithm.HS256)

  private def tokenLifetime(user: AuthUser): Long =
    if user.technical then cfg.userTechnicalTokenLifetimeDays.toLong * 24 * 3600
    else if user.role == "D" then cfg.userDemoTokenLifetimeDays.toLong * 24 * 3600
    else cfg.userTokenLifetimeHours.toLong * 3600

  /**
   * Hash ID that changes when the user's role or sites change.
   * A change forces client-side session expiry (frontend checks on startup).
   */
  private def userHashId(user: AuthUser): String =
    sha256hex(s"${user.id}|${user.role}|${user.status.code}")

  /** Hash ID that changes when the tenant's active components change. */
  private def tenantHashId(tenant: AuthTenant): String =
    sha256hex(s"${tenant.id}|${tenant.activeComponents.sorted.mkString(",")}")

  private def sha256hex(s: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_)).mkString

  /** Static ACL list derived from role code (full Casbin policy is in ev-auth-core). */
  private def roleToAcl(role: String): List[String] = role match
    case "A" | "SA" => List("*")
    case "B"        => List("ChargingStation:Read", "Transaction:Read", "Tag:Read")
    case "D"        => List("ChargingStation:Read", "Transaction:Read")
    case _          => Nil

  private def roleToScopes(role: String): List[String] = role match
    case "A" | "SA" => List("admin", "read", "write")
    case "B"        => List("read", "write")
    case "D"        => List("read")
    case _          => List("read")

object TokenService:

  val live: ZLayer[
    AuthConfig & UserRepository & TagRepository & UserSiteRepository & TenantRepository,
    Nothing,
    TokenService
  ] =
    ZLayer.fromFunction(new TokenService(_, _, _, _, _))
