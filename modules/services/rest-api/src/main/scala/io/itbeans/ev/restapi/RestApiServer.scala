package io.itbeans.ev.restapi

import io.itbeans.ev.auth.AuthorizationService
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import zio._
import zio.http.{Routes, Server}

// ---------------------------------------------------------------------------
// RestApiServer — wires all Tapir endpoint groups into a single ZIO HTTP server.
// ---------------------------------------------------------------------------

final class RestApiServer(
    cfg: RestApiConfig,
    repo: RestApiRepository,
    auth: AuthorizationService,
    gatewayClient: OcppGatewayGrpcClient,
    billingClient: BillingGrpcClient
):

  def start: Task[Unit] =
    val allEndpoints =
      ChargingStationEndpoints.routes(repo) ++
        TransactionEndpoints.routes(repo) ++
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

    Server.serve(docsRoutes ++ apiRoutes)
      .provide(Server.defaultWithPort(cfg.httpPort))
      .unit

object RestApiServer:

  val live: ZLayer[
    RestApiConfig & RestApiRepository & AuthorizationService & OcppGatewayGrpcClient & BillingGrpcClient,
    Nothing,
    RestApiServer
  ] =
    ZLayer.fromFunction(RestApiServer(_, _, _, _, _))
