package io.itbeans.ev.authservice

import io.grpc.netty.NettyServerBuilder
import io.itbeans.ev.auth.grpc.auth_service._
import io.itbeans.ev.otel.EvTracing
import zio._

import scala.concurrent.{ExecutionContext, Future}

final class AuthGrpcTransport(handler: AuthGrpcHandler, tracing: EvTracing, rt: Runtime[Any])
    extends AuthServiceGrpc.AuthService:

  private def run[A](spanName: String)(effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(tracing.spanTask(spanName)(effect)))

  override def validateToken(req: ValidateTokenRequest): Future[ValidateTokenResponse] =
    run("auth.validateToken")(
      handler.validateToken(ValidateTokenRequestADT(req.jwt)).map { r =>
        ValidateTokenResponse(
          valid = r.valid,
          error = r.error.getOrElse(""),
          userTokenJson = r.userTokenJson
        )
      }
    )

  override def resolveOcppAuthorization(req: OcppAuthorizationRequest): Future[OcppAuthorizationResponse] =
    run("auth.resolveOcppAuthorization")(
      handler
        .resolveOcppAuthorize(
          ResolveOcppAuthorizeRequestADT(
            tenantId = req.tenantId,
            tagId = req.idTag,
            chargingStationId = req.chargingStationId
          )
        )
        .map { r =>
          val userId = r.userJson
            .flatMap(json => io.circe.parser.parse(json).toOption)
            .flatMap(_.hcursor.downField("id").as[String].toOption)
            .getOrElse("")
          OcppAuthorizationResponse(status = r.status, userId = userId)
        }
    )

  override def resolveTenant(req: ResolveTenantRequest): Future[ResolveTenantResponse] =
    run("auth.resolveTenant")(
      handler.resolveTenant(ResolveTenantRequestADT(req.subdomain)).map { r =>
        ResolveTenantResponse(found = r.found, tenantId = r.tenantId.getOrElse(""))
      }
    )

object AuthGrpcTransport:

  val start: RIO[AuthGrpcHandler & AuthConfig & EvTracing, Unit] =
    for
      handler <- ZIO.service[AuthGrpcHandler]
      cfg     <- ZIO.service[AuthConfig]
      tracing <- ZIO.service[EvTracing]
      rt      <- ZIO.runtime[Any]
      impl = new AuthGrpcTransport(handler, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(AuthServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"Auth gRPC server listening on :${server.getPort}")
    yield ()
