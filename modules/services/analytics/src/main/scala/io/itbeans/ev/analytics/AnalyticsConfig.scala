package io.itbeans.ev.analytics

import zio._

// ---------------------------------------------------------------------------
// AnalyticsConfig — ZIO Config for ev-analytics.
// ---------------------------------------------------------------------------

case class AnalyticsConfig(
    httpPort: Int,

    // TimescaleDB (PostgreSQL extension) — analytics hypertables
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassword: String,

    // Log retention: records older than this are dropped by TimescaleDB compression
    logRetentionDays: Int,         // default 90
    consumptionRetentionDays: Int, // default 365

    // Default tenant for single-tenant / dev deployments
    defaultTenantId: String
)

object AnalyticsConfig:

  given Config[AnalyticsConfig] = (
    Config.int("httpPort") zip
      Config.string("jdbcUrl") zip
      Config.string("jdbcUser") zip
      Config.string("jdbcPassword") zip
      Config.int("logRetentionDays") zip
      Config.int("consumptionRetentionDays") zip
      Config.string("defaultTenantId")
  ).nested("analytics").map {
    case (httpPort, jdbcUrl, jdbcUser, jdbcPassword, logRet, consRet, tenantId) =>
      AnalyticsConfig(httpPort, jdbcUrl, jdbcUser, jdbcPassword, logRet, consRet, tenantId)
  }
