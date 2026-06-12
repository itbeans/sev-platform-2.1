package io.itbeans.ev.restapi

import io.itbeans.ev.auth.AuthorizationService
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import zio._
import zio.http._

// ---------------------------------------------------------------------------
// RestApiServer — wires all Tapir endpoint groups into a single ZIO HTTP server.
//
// Authentication: every /api route requires a Bearer JWT, validated against
// ev-auth-service over gRPC (fail-closed — 503 if auth-service unreachable).
// Authorization (roles/resources) is enforced in-process by ev-auth-core.
// ---------------------------------------------------------------------------

final class RestApiServer(
    cfg: RestApiConfig,
    repo: RestApiRepository,
    auth: AuthorizationService,
    authClient: AuthGrpcClient,
    gatewayClient: OcppGatewayGrpcClient,
    billingClient: BillingGrpcClient
):

  // Rejects requests without a valid Bearer JWT. Validation is delegated to
  // ev-auth-service so revoked sessions are honoured immediately.
  private val bearerAuth: HandlerAspect[Any, Unit] =
    HandlerAspect.interceptIncomingHandler {
      Handler.fromFunctionZIO[Request] { req =>
        req.header(Header.Authorization) match
          case Some(Header.Authorization.Bearer(token)) =>
            authClient
              .validateToken(token.value.mkString)
              .mapError { err =>
                Response
                  .text(s"Auth service unavailable")
                  .status(Status.ServiceUnavailable)
              }
              .filterOrFail(_.valid)(Response.status(Status.Unauthorized))
              .as((req, ()))
          case _ =>
            ZIO.fail(Response.status(Status.Unauthorized))
      }
    }

  def start: Task[Unit] =
    val allEndpoints =
      ChargingStationEndpoints.routes(repo) ++
        TransactionEndpoints.routes(repo, gatewayClient) ++
        UserEndpoints.routes(repo) ++
        SiteEndpoints.routes(repo) ++
        CompanyEndpoints.routes(repo) ++
        TagEndpoints.routes(repo) ++
        AssetEndpoints.routes(repo) ++
        SettingsEndpoints.routes(repo) ++
        BillingEndpoints.routes(billingClient) ++
        OcppCommandEndpoints.routes(repo, gatewayClient) ++
        SmartChargingProfileEndpoints.routes(repo) ++
        NotificationEndpoints.routes(repo) ++
        RoamingEndpoints.routes(repo) ++
        StatisticsEndpoints.routes(cfg, repo) ++
        LogsEndpoints.routes(cfg, repo)

    val swaggerEndpoints = SwaggerInterpreter()
      .fromServerEndpoints[Task](allEndpoints, "ev-server REST API", "2.0")

    val apiRoutes: Routes[Any, zio.http.Response] =
      ZioHttpInterpreter().toHttp(allEndpoints)

    val docsRoutes: Routes[Any, zio.http.Response] =
      ZioHttpInterpreter().toHttp(swaggerEndpoints)

    val healthRoutes: Routes[Any, zio.http.Response] =
      Routes(Method.GET / "health" -> handler(Response.text("OK")))

    Server.serve(healthRoutes ++ docsRoutes ++ (apiRoutes @@ bearerAuth))
      .provide(Server.defaultWithPort(cfg.httpPort))
      .unit

object RestApiServer:

  val live: ZLayer[
    RestApiConfig & RestApiRepository & AuthorizationService & AuthGrpcClient & OcppGatewayGrpcClient &
      BillingGrpcClient,
    Nothing,
    RestApiServer
  ] =
    ZLayer.fromFunction(RestApiServer(_, _, _, _, _, _))
