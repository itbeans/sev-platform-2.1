package io.itbeans.ev.car

import zio.Config

// ---------------------------------------------------------------------------
// CarConfig — ZIO Config for ev-car service.
//
// HOCON layout (application.conf / env overrides):
//
//   car {
//     evDatabase {
//       url  = "https://ev-database.org/api"
//       key  = "..."
//     }
//
//     # Sync schedule — standard ISO-8601 duration string, e.g. "PT4H"
//     syncInterval = "PT4H"
//     syncInterval = ${?CAR_SYNC_INTERVAL}
//
//     connectors {
//       mercedes {
//         enabled           = false
//         clientId          = ""
//         clientSecret      = ""
//         authenticationUrl = ""
//         apiUrl            = ""
//       }
//       tronity {
//         enabled      = false
//         clientId     = ""
//         clientSecret = ""
//         apiUrl       = ""
//       }
//       targaTelematics {
//         enabled           = false
//         clientId          = ""
//         clientSecret      = ""
//         authenticationUrl = ""
//         apiUrl            = ""
//       }
//     }
//   }
// ---------------------------------------------------------------------------

case class EvDatabaseConfig(
    url: String,
    key: String
)

case class MercedesConnectorConfig(
    enabled: Boolean,
    clientId: String,
    clientSecret: String,
    authenticationUrl: String,
    apiUrl: String
)

case class TronityConnectorConfig(
    enabled: Boolean,
    clientId: String,
    clientSecret: String,
    apiUrl: String
)

case class TargaTelematicsConnectorConfig(
    enabled: Boolean,
    clientId: String,
    clientSecret: String,
    authenticationUrl: String,
    apiUrl: String
)

case class CarConnectorConfig(
    mercedes: MercedesConnectorConfig,
    tronity: TronityConnectorConfig,
    targaTelematics: TargaTelematicsConnectorConfig
)

case class CarConfig(
    grpcPort: Int,
    evDatabase: EvDatabaseConfig,
    syncInterval: String,
    connectors: CarConnectorConfig
)

object CarConfig:

  private val evDbConfig: Config[EvDatabaseConfig] = (
    Config.string("url") zip Config.string("key")
  ).map { case (url, key) => EvDatabaseConfig(url, key) }

  private val mercedesConfig: Config[MercedesConnectorConfig] = (
    Config.boolean("enabled") zip Config.string("clientId") zip Config.string("clientSecret") zip
      Config.string("authenticationUrl") zip Config.string("apiUrl")
  ).map { case (en, cid, cs, au, api) => MercedesConnectorConfig(en, cid, cs, au, api) }

  private val tronityConfig: Config[TronityConnectorConfig] = (
    Config.boolean("enabled") zip Config.string("clientId") zip Config.string("clientSecret") zip
      Config.string("apiUrl")
  ).map { case (en, cid, cs, api) => TronityConnectorConfig(en, cid, cs, api) }

  private val targaConfig: Config[TargaTelematicsConnectorConfig] = (
    Config.boolean("enabled") zip Config.string("clientId") zip Config.string("clientSecret") zip
      Config.string("authenticationUrl") zip Config.string("apiUrl")
  ).map { case (en, cid, cs, au, api) => TargaTelematicsConnectorConfig(en, cid, cs, au, api) }

  private val connectorsConfig: Config[CarConnectorConfig] = (
    mercedesConfig.nested("mercedes") zip
      tronityConfig.nested("tronity") zip
      targaConfig.nested("targaTelematics")
  ).map { case (mercedes, tronity, targa) => CarConnectorConfig(mercedes, tronity, targa) }

  given Config[CarConfig] = (
    Config.int("grpcPort") zip
      evDbConfig.nested("evDatabase") zip
      Config.string("syncInterval") zip
      connectorsConfig.nested("connectors")
  ).nested("car").map {
    case (grpcPort, evDb, syncInterval, connectors) => CarConfig(grpcPort, evDb, syncInterval, connectors)
  }
