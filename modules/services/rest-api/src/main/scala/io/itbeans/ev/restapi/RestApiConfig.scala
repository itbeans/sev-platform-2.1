package io.itbeans.ev.restapi

import zio._

// ---------------------------------------------------------------------------
// RestApiConfig — ZIO Config for ev-rest-api.
// ---------------------------------------------------------------------------

case class RestApiConfig(
    httpPort: Int,
    mongoUri: String,
    mongoDbName: String,
    jwtSecret: String,
    authGrpcHost: String,
    authGrpcPort: Int,
    ocppGatewayGrpcHost: String,
    ocppGatewayGrpcPort: Int,
    billingGrpcHost: String,
    billingGrpcPort: Int,
    analyticsBaseUrl: String
)

object RestApiConfig:

  given Config[RestApiConfig] = (
    Config.int("httpPort") zip
      Config.string("mongoUri") zip
      Config.string("mongoDbName") zip
      Config.string("jwtSecret") zip
      Config.string("authGrpcHost") zip
      Config.int("authGrpcPort") zip
      Config.string("ocppGatewayGrpcHost") zip
      Config.int("ocppGatewayGrpcPort") zip
      Config.string("billingGrpcHost") zip
      Config.int("billingGrpcPort") zip
      Config.string("analyticsBaseUrl")
  ).nested("restApi").map {
    case (
          httpPort,
          mongoUri,
          mongoDbName,
          jwtSecret,
          authHost,
          authPort,
          gwHost,
          gwPort,
          billingHost,
          billingPort,
          analyticsUrl
        ) =>
      RestApiConfig(
        httpPort,
        mongoUri,
        mongoDbName,
        jwtSecret,
        authHost,
        authPort,
        gwHost,
        gwPort,
        billingHost,
        billingPort,
        analyticsUrl
      )
  }
