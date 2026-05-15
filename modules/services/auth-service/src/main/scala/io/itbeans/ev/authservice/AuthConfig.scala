package io.itbeans.ev.authservice

import zio._

// ---------------------------------------------------------------------------
// AuthConfig — ZIO Config derivation for ev-auth-service.
// ---------------------------------------------------------------------------

case class AuthConfig(
    httpPort: Int,
    grpcPort: Int,
    // JWT signing secret — MUST match the TypeScript `userTokenKey` value.
    userTokenKey: String,
    // Token lifetimes (matching TypeScript CentralSystemRestServiceConfiguration)
    userTokenLifetimeHours: Int,
    userDemoTokenLifetimeDays: Int,
    userTechnicalTokenLifetimeDays: Int,
    // Account lockout settings
    passwordWrongNumberOfTrial: Int,
    passwordBlockedWaitTimeMin: Int,
    // The super-admin tenant ID (used when no subdomain is provided)
    defaultTenantId: String,
    // Enable the parallel-run comparison endpoint (set true during 60-day migration)
    parallelRunEnabled: Boolean = false
)

object AuthConfig:

  given Config[AuthConfig] = (
    Config.int("httpPort") zip
      Config.int("grpcPort") zip
      Config.string("userTokenKey") zip
      Config.int("userTokenLifetimeHours") zip
      Config.int("userDemoTokenLifetimeDays") zip
      Config.int("userTechnicalTokenLifetimeDays") zip
      Config.int("passwordWrongNumberOfTrial") zip
      Config.int("passwordBlockedWaitTimeMin") zip
      Config.string("defaultTenantId") zip
      Config.boolean("parallelRunEnabled").withDefault(false)
  ).nested("auth").map {
    case (httpPort, grpcPort, userTokenKey, utlh, udtld, uttld, pwnt, pbwt, tenantId, prEnabled) =>
      AuthConfig(httpPort, grpcPort, userTokenKey, utlh, udtld, uttld, pwnt, pbwt, tenantId, prEnabled)
  }
