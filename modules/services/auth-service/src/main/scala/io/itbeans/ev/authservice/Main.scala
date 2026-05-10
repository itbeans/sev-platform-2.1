package io.itbeans.ev.authservice

import io.itbeans.ev.mongo.{MongoClientLayer, MongoConfig, MongoDatabaseLayer}
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-auth-service entrypoint.
//
// Starts:
//   - HTTP REST API (Tapir → ZIO HTTP) on port 8080
//     POST /auth/signin      — login, returns JWT
//     POST /auth/signout     — stateless logout
//     POST /auth/check-token — token validation for non-gRPC clients
//   - gRPC server on port 9090
//     ValidateToken, ResolveOcppAuthorize, ResolveTenant
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[MongoConfig]),
      ZLayer.fromZIO(ZIO.config[AuthConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      // ── OpenTelemetry ──────────────────────────────────────────────────
      OtelLayer.live,
      EvTracing.live,
      // ── Infrastructure ─────────────────────────────────────────────────
      MongoClientLayer.live,
      MongoDatabaseLayer.live,
      // ── Repository layers ──────────────────────────────────────────────
      MongoUserRepository.live,
      MongoTenantRepository.live,
      MongoTagRepository.live,
      MongoUserSiteRepository.live,
      MongoParallelRunRepository.live,
      // ── Application layers ─────────────────────────────────────────────
      TokenService.live,
      ParallelRunService.live,
      AuthGrpcHandler.live,
      // ── HTTP server (port 8080) ─────────────────────────────────────────
      Server.defaultWithPort(8080)
    )

  private val program: ZIO[
    UserRepository & TenantRepository & TagRepository & TokenService &
      ParallelRunService & AuthGrpcHandler & Server & AuthConfig & EvTracing,
    Throwable,
    Unit
  ] =
    for
      cfg            <- ZIO.service[AuthConfig]
      userRepo       <- ZIO.service[UserRepository]
      tenantRepo     <- ZIO.service[TenantRepository]
      tokenSvc       <- ZIO.service[TokenService]
      parallelRunSvc <- ZIO.service[ParallelRunService]
      _ <- ZIO.logInfo(
        s"ev-auth-service starting: http=:${cfg.httpPort} grpc=:${cfg.grpcPort} " +
          s"parallelRun=${cfg.parallelRunEnabled}"
      )
      _ <- AuthGrpcTransport.start
      parallelRunOpt = if cfg.parallelRunEnabled then Some(parallelRunSvc) else None
      _ <- Server.serve(AuthHttpServer.routes(userRepo, tenantRepo, tokenSvc, parallelRunOpt, cfg))
    yield ()
