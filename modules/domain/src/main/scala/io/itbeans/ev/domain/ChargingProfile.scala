package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type ChargingProfileId = String

object ChargingProfileId:
  def apply(v: String): ChargingProfileId             = v
  def unapply(id: ChargingProfileId): Some[String]    = Some(id)
  extension (id: ChargingProfileId) def value: String = id

  given Schema[ChargingProfileId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(ChargingProfileId(_), _.value)

// ---------------------------------------------------------------------------
// ChargingProfile domain — OCPP 1.6 / 2.x charging schedules pushed to stations.
// Mirrors the TypeScript ChargingProfile / SmartChargingProfile storage model.
// ---------------------------------------------------------------------------

// One time-slot within a charging schedule
case class ChargingSchedulePeriod(
    startPeriod: Int,   // seconds from schedule start
    limitWatts: Double, // power limit for this period
    numberPhases: Option[Int]
)

case class ChargingSchedule(
    startSchedule: Option[Instant],
    durationSecs: Option[Int],
    chargingRateUnit: String, // "W" or "A"
    periods: List[ChargingSchedulePeriod],
    minChargingRate: Option[Double]
)

case class ChargingProfileData(
    chargingProfileId: Int,
    transactionId: Option[Long],
    stackLevel: Int,
    chargingProfilePurpose: String, // TxDefaultProfile | TxProfile | ChargePointMaxProfile
    chargingProfileKind: String,    // Absolute | Recurring | Relative
    recurrencyKind: Option[String], // Daily | Weekly
    validFrom: Option[Instant],
    validTo: Option[Instant],
    chargingSchedule: ChargingSchedule
)

case class ChargingProfile(
    id: ChargingProfileId,
    tenantId: TenantId,
    chargingStationId: ChargingStationId,
    connectorId: Int,
    siteAreaId: Option[SiteAreaId],
    siteId: Option[SiteId],
    profile: ChargingProfileData,
    createdOn: Instant,
    lastChangedOn: Instant
)

object ChargingProfile:
  given Schema[ChargingSchedulePeriod] = DeriveSchema.gen
  given Schema[ChargingSchedule] = DeriveSchema.gen
  given Schema[ChargingProfileData] = DeriveSchema.gen
  given Schema[ChargingProfile] = DeriveSchema.gen
