package io.itbeans.ev.authservice

import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import zio._

// ---------------------------------------------------------------------------
// AuthGrpcServer — pre-codegen ADTs and handler for the Auth gRPC API.
//
// When ScalaPB code generation is wired up, the ADTs below will be replaced
// by generated protobuf message classes from auth_service.proto.
// The handler is kept transport-free for unit testing.
//
// gRPC contract (auth_service.proto):
//   ValidateToken         — decode + validate a JWT, return the UserToken
//   ResolveOcppAuthorize  — OCPP Authorize.req (badge/tag → ACCEPTED|INVALID)
//   ResolveTenant         — subdomain lookup (called by ev-rest-api, ev-ocpp-*)
// ---------------------------------------------------------------------------

// ── Pre-codegen request/response ADTs ─────────────────────────────────────

case class ValidateTokenRequestADT(rawToken: String)

case class ValidateTokenResponseADT(
    valid: Boolean,
    userTokenJson: String, // JSON-encoded UserToken (empty if invalid)
    error: Option[String]
)

case class ResolveOcppAuthorizeRequestADT(
    tenantId: String,
    tagId: String,
    chargingStationId: String // for access-control checks (reserved for future use)
)

case class ResolveOcppAuthorizeResponseADT(
    status: String,          // "Accepted" | "Invalid" | "Blocked" | "Expired"
    userJson: Option[String] // JSON-encoded AuthUser (None if tag unknown/invalid)
)

case class ResolveTenantRequestADT(subdomain: String)

case class ResolveTenantResponseADT(
    found: Boolean,
    tenantId: Option[String],
    tenantJson: Option[String] // JSON-encoded AuthTenant
)

// ── Handler ───────────────────────────────────────────────────────────────

final class AuthGrpcHandler(
    tokenSvc: TokenService,
    userRepo: UserRepository,
    tenantRepo: TenantRepository,
    tagRepo: TagRepository,
    cfg: AuthConfig
):

  /**
   * Validate a JWT and return its decoded payload.
   * Called by ev-rest-api, ev-ocpp-processor on every authenticated request.
   */
  def validateToken(req: ValidateTokenRequestADT): Task[ValidateTokenResponseADT] =
    tokenSvc.validateToken(req.rawToken)
      .map { t =>
        ValidateTokenResponseADT(
          valid = true,
          userTokenJson = t.asJson.noSpaces,
          error = None
        )
      }
      .catchAll { err =>
        ZIO.succeed(ValidateTokenResponseADT(
          valid = false,
          userTokenJson = "",
          error = Some(err.getMessage)
        ))
      }

  /**
   * Resolve OCPP Authorize.req: badge → user → ACCEPTED/INVALID.
   *
   * OCPP Authorize flow:
   *   1. Look up tag by idTag in tenant's tags collection
   *   2. Tag must exist, be active, and have an associated user
   *   3. User must be in Active status
   *   4. Return Accepted; on any failure return Invalid
   */
  def resolveOcppAuthorize(req: ResolveOcppAuthorizeRequestADT): Task[ResolveOcppAuthorizeResponseADT] =
    val tenantId = TenantId(req.tenantId)
    (for
      tag <- tagRepo.findById(tenantId, req.tagId).flatMap {
        case None    => ZIO.fail(new Exception(s"Tag ${req.tagId} not found"))
        case Some(t) => ZIO.succeed(t)
      }
      _ <- if !tag.active then ZIO.fail(new Exception(s"Tag ${req.tagId} is inactive"))
      else ZIO.unit
      uid <- tag.userID match
        case None    => ZIO.fail(new Exception(s"Tag ${req.tagId} has no associated user"))
        case Some(u) => ZIO.succeed(u)
      user <- userRepo.findById(tenantId, uid).flatMap {
        case None    => ZIO.fail(new Exception(s"User $uid not found"))
        case Some(u) => ZIO.succeed(u)
      }
      _ <- user.status match
        case AuthUserStatus.Active  => ZIO.unit
        case AuthUserStatus.Blocked => ZIO.fail(new Exception("User blocked"))
        case AuthUserStatus.Locked  => ZIO.fail(new Exception("User locked"))
        case other                  => ZIO.fail(new Exception(s"User status: ${other.code}"))
    yield ResolveOcppAuthorizeResponseADT(
      status = "Accepted",
      userJson = Some(user.asJson.noSpaces)
    )).catchAll { err =>
      ZIO.logWarning(s"OCPP Authorize failed for tag ${req.tagId}: ${err.getMessage}") *>
        ZIO.succeed(ResolveOcppAuthorizeResponseADT(status = "Invalid", userJson = None))
    }

  /**
   * Resolve tenant by subdomain.
   * Called by ev-ocpp-gateway when a charging station connects (host header → tenantId).
   */
  def resolveTenant(req: ResolveTenantRequestADT): Task[ResolveTenantResponseADT] =
    val lookup =
      if req.subdomain.isBlank then tenantRepo.findById(cfg.defaultTenantId)
      else tenantRepo.findBySubdomain(req.subdomain)
    lookup.map {
      case None => ResolveTenantResponseADT(found = false, tenantId = None, tenantJson = None)
      case Some(t) => ResolveTenantResponseADT(
          found = true,
          tenantId = Some(t.id),
          tenantJson = Some(t.asJson.noSpaces)
        )
    }

object AuthGrpcHandler:

  val live: ZLayer[
    TokenService & UserRepository & TenantRepository & TagRepository & AuthConfig,
    Nothing,
    AuthGrpcHandler
  ] =
    ZLayer.fromFunction(new AuthGrpcHandler(_, _, _, _, _))
