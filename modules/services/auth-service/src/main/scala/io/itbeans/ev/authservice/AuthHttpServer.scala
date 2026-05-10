package io.itbeans.ev.authservice

import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._
import zio.http.{Response, Routes}

import java.time.{Instant, ZoneOffset, ZonedDateTime}

// ---------------------------------------------------------------------------
// AuthHttpServer — wires Tapir auth endpoints to ZIO HTTP routes.
//
// Login flow (POST /auth/signin):
//   1. Resolve tenant by subdomain (empty → default super-admin tenant)
//   2. Find user by email in that tenant
//   3. Check lockout — block if trials ≥ threshold and within block window
//   4. Verify BCrypt password
//   5. Check user status (Active only can log in via HTTP)
//   6. Technical users cannot log in via UI
//   7. Issue JWT (with embedded tags + sites)
//   8. Return { token }
// ---------------------------------------------------------------------------

object AuthHttpServer:

  def routes(
      userRepo: UserRepository,
      tenantRepo: TenantRepository,
      tokenSvc: TokenService,
      parallelRunSvc: Option[ParallelRunService],
      cfg: AuthConfig
  ): Routes[Any, Response] =
    val coreEndpoints = List(
      AuthEndpoints.signIn.zServerLogic { body =>
        signIn(body, userRepo, tenantRepo, tokenSvc, cfg).mapError(_.getMessage)
      },
      AuthEndpoints.signOut.zServerLogic { _ =>
        ZIO.unit // stateless — client discards the token
      },
      AuthEndpoints.checkToken.zServerLogic { body =>
        tokenSvc.validateToken(body.token)
          .map(t => CheckTokenResponse(valid = true, userTokenJson = Some(t.asJson.noSpaces)))
          .catchAll(_ => ZIO.succeed(CheckTokenResponse(valid = false, userTokenJson = None)))
      }
    )
    val parallelRunEndpoints = parallelRunSvc.toList.flatMap { svc =>
      List(
        ParallelRunEndpoints.compare.zServerLogic { req =>
          svc.compare(req.tenant, req.email, req.tsToken)
            .map(r => ParallelRunCompareResponse(r.matched, r.diffCount, r.diffs))
            .mapError(_.getMessage)
        },
        ParallelRunEndpoints.report.zServerLogic { case (tenant, fromMs, toMs) =>
          val from = Instant.ofEpochMilli(fromMs)
          val to   = Instant.ofEpochMilli(toMs)
          svc.buildReport(tenant, from, to).mapError(_.getMessage)
        }
      )
    }
    ZioHttpInterpreter().toHttp(coreEndpoints ++ parallelRunEndpoints)

  // ── Login logic ───────────────────────────────────────────────────────────

  private def signIn(
      body: SignInBody,
      userRepo: UserRepository,
      tenantRepo: TenantRepository,
      tokenSvc: TokenService,
      cfg: AuthConfig
  ): Task[SignInResponse] =
    for
      // 1. Resolve tenant
      tenant <- resolveTenant(body.tenant, tenantRepo, cfg)
      tenantId = TenantId(tenant.id)

      // 2. Find user by email
      user <- userRepo.findByEmail(tenantId, body.email).flatMap {
        case None    => ZIO.fail(new Exception(s"Invalid credentials"))
        case Some(u) => ZIO.succeed(u)
      }

      // 3. Check account lockout
      _ <- checkLockout(user, cfg)

      // 4. Verify password
      pwOk <- tokenSvc.checkPassword(body.password, user.password)
      _ <- if pwOk then ZIO.unit
      else handleWrongPassword(user, tenantId, userRepo, cfg)

      // 5. Check user status (only Active can proceed)
      _ <- user.status match
        case AuthUserStatus.Active   => ZIO.unit
        case AuthUserStatus.Pending  => ZIO.fail(new Exception("Email not verified"))
        case AuthUserStatus.Inactive => ZIO.fail(new Exception("Account inactive"))
        case AuthUserStatus.Blocked  => ZIO.fail(new Exception("Account blocked"))
        case AuthUserStatus.Locked   => ZIO.fail(new Exception("Account locked"))

      // 6. Technical users cannot log in via UI
      _ <- if user.technical then
        ZIO.fail(new Exception("Technical accounts cannot log in via the portal"))
      else ZIO.unit

      // 7. Reset wrong-trial counter on successful login
      _ <- userRepo.updateLockout(tenantId, user.id, 0, None)

      // 8. Issue JWT
      token <- tokenSvc.issueToken(tenantId, user, tenant)
    yield SignInResponse(token)

  private def resolveTenant(
      subdomain: String,
      tenantRepo: TenantRepository,
      cfg: AuthConfig
  ): Task[AuthTenant] =
    if subdomain.isBlank then
      // Empty subdomain → super-admin tenant (must exist in tenants collection)
      tenantRepo.findById(cfg.defaultTenantId).flatMap {
        case None    => ZIO.fail(new Exception(s"Default tenant '${cfg.defaultTenantId}' not found"))
        case Some(t) => ZIO.succeed(t)
      }
    else
      tenantRepo.findBySubdomain(subdomain).flatMap {
        case None    => ZIO.fail(new Exception(s"Tenant '$subdomain' not found"))
        case Some(t) => ZIO.succeed(t)
      }

  private def checkLockout(user: AuthUser, cfg: AuthConfig): Task[Unit] =
    user.passwordBlockedUntil match
      case Some(until) if Instant.now().isBefore(until) =>
        ZIO.fail(new Exception("Account temporarily locked due to too many wrong password attempts"))
      case Some(_) =>
        // Lockout window has passed — the counter will be reset on next successful login
        ZIO.unit
      case None => ZIO.unit

  private def handleWrongPassword(
      user: AuthUser,
      tenantId: TenantId,
      userRepo: UserRepository,
      cfg: AuthConfig
  ): Task[Nothing] =
    val newTrials = user.passwordWrongNbrTrials + 1
    val blockedUntil =
      if newTrials >= cfg.passwordWrongNumberOfTrial then
        Some(Instant.now().plusSeconds(cfg.passwordBlockedWaitTimeMin.toLong * 60))
      else None
    userRepo.updateLockout(tenantId, user.id, newTrials, blockedUntil) *>
      ZIO.fail(new Exception("Invalid credentials"))
