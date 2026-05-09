package io.itbeans.ev.asset

import zio._

// ---------------------------------------------------------------------------
// AssetConfig — ZIO Config derivation for ev-asset service.
// ---------------------------------------------------------------------------

case class AssetConfig(
    httpPort: Int,
    pollIntervalSeconds: Int,
    tenantIds: List[String]
)

object AssetConfig:

  given Config[AssetConfig] = (
    Config.int("httpPort") zip
      Config.int("pollIntervalSeconds") zip
      Config.string("tenantIds").map(_.split(",").toList.map(_.trim))
  ).nested("asset").map {
    case (httpPort, pollInterval, tenants) =>
      AssetConfig(httpPort, pollInterval, tenants)
  }
