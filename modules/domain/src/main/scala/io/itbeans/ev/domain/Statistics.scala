package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema}
import java.time.Instant

// ---------------------------------------------------------------------------
// Statistics domain — aggregated consumption and session metrics surfaced by
// the ev-analytics service and the REST API /Statistics endpoints.
// ---------------------------------------------------------------------------

enum StatisticScope:
  case Tenant, Site, SiteArea, ChargingStation, User

enum StatisticDataType:
  case Consumption // kWh
  case Usage       // session count
  case Inactivity  // idle time percentage
  case Pricing     // revenue
  case Sessions    // raw session count
  case ActiveSessions

enum StatisticUnit:
  case Kilowatt, KilowattHour, Ampere, Volt, Hour, Minute, Second, Currency, Count, Percent

// One data point in a time-series or category breakdown
case class StatPoint(
    label: String,
    period: String, // "YYYY-MM-DD", "YYYY-WW", "YYYY-MM", etc.
    value: Double
)

// Complete statistics result returned from the analytics service or TimescaleDB
case class StatisticsResult(
    tenantId: TenantId,
    dataType: StatisticDataType,
    dataScope: StatisticScope,
    unit: StatisticUnit,
    total: Double,
    data: List[StatPoint],
    fromDate: Instant,
    toDate: Instant
)

// Aggregated consumption record written to TimescaleDB hypertable
case class ConsumptionIngestRecord(
    transactionId: Long,
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    siteId: Option[SiteId],
    siteAreaId: Option[SiteAreaId],
    connectorId: Int,
    timestamp: Instant,
    consumptionWh: Double,
    instantWatts: Option[Double],
    stateOfCharge: Option[Int],
    pricingSource: Option[String],
    cumulatedPrice: Option[Double]
)

object Statistics:
  given Schema[StatisticScope] = DeriveSchema.gen
  given Schema[StatisticDataType] = DeriveSchema.gen
  given Schema[StatisticUnit] = DeriveSchema.gen
  given Schema[StatPoint] = DeriveSchema.gen
  given Schema[StatisticsResult] = DeriveSchema.gen
  given Schema[ConsumptionIngestRecord] = DeriveSchema.gen
