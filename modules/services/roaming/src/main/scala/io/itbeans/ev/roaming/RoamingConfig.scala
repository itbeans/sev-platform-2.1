package io.itbeans.ev.roaming

import zio._

// ---------------------------------------------------------------------------
// RoamingConfig — ZIO Config derivation for ev-roaming service.
// ---------------------------------------------------------------------------

case class RoamingConfig(
    httpPort: Int,
    // OCPI identity (who we are in the roaming network)
    ocpiCountryCode: String, // ISO 3166-1 alpha-2 (e.g. "DE")
    ocpiPartyId: String,     // 3-character OCPI party ID (e.g. "EVX")
    ocpiOperatorName: String,
    ocpiLocalToken: String, // token we use to authenticate inbound OCPI calls
    // External URL base (how partners reach us)
    externalUrl: String, // e.g. "https://csms.example.com"
    // Default tenant (for credential exchange with unknown tenant context)
    defaultTenantId: String
)

object RoamingConfig:

  given Config[RoamingConfig] = (
    Config.int("httpPort") zip
      Config.string("ocpiCountryCode") zip
      Config.string("ocpiPartyId") zip
      Config.string("ocpiOperatorName") zip
      Config.string("ocpiLocalToken") zip
      Config.string("externalUrl") zip
      Config.string("defaultTenantId")
  ).nested("roaming").map {
    case (httpPort, countryCode, partyId, opName, localToken, extUrl, tenantId) =>
      RoamingConfig(httpPort, countryCode, partyId, opName, localToken, extUrl, tenantId)
  }
