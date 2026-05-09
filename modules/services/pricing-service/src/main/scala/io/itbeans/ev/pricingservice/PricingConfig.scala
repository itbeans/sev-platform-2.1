package io.itbeans.ev.pricingservice

import zio._

// ---------------------------------------------------------------------------
// PricingConfig — ZIO Config derivation for ev-pricing-service.
//
// All values come from application.conf and can be overridden with env vars
// via substitution syntax: ${?ENV_VAR_NAME}.
// ---------------------------------------------------------------------------

case class PricingConfig(
    grpcPort: Int,
    httpPort: Int,
    defaultCurrency: String
)

object PricingConfig:

  given Config[PricingConfig] = (
    Config.int("grpcPort") zip Config.int("httpPort") zip Config.string("defaultCurrency")
  ).nested("pricing").map { case (grpc, http, currency) => PricingConfig(grpc, http, currency) }
