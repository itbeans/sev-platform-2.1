package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type AssetId = String

object AssetId:
  def apply(v: String): AssetId             = v
  def unapply(id: AssetId): Some[String]    = Some(id)
  extension (id: AssetId) def value: String = id
  given Schema[AssetId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(AssetId(_), _.value)

// ---------------------------------------------------------------------------
// Asset domain — solar panels, batteries, building load meters, etc.
// Mirrors the TypeScript Asset / AssetConnection storage model.
// ---------------------------------------------------------------------------

enum AssetType(val code: String):
  case Consumption           extends AssetType("CO")
  case Production            extends AssetType("PR")
  case ConsumptionProduction extends AssetType("CO-PR")

// External system that provides asset telemetry (push or pull)
enum AssetConnectorType(val code: String):
  case Greencom  extends AssetConnectorType("greencom")
  case IOThink   extends AssetConnectorType("iothink")
  case Lacroix   extends AssetConnectorType("lacroix")
  case Schneider extends AssetConnectorType("schneider")
  case Wit       extends AssetConnectorType("wit")

case class AssetMeasurement(
    timestamp: Instant,
    instantWatts: Double,
    instantWattsL1: Option[Double],
    instantWattsL2: Option[Double],
    instantWattsL3: Option[Double],
    instantAmps: Option[Double],
    instantAmpsL1: Option[Double],
    instantAmpsL2: Option[Double],
    instantAmpsL3: Option[Double],
    instantVolts: Option[Double],
    instantVoltsL1: Option[Double],
    instantVoltsL2: Option[Double],
    instantVoltsL3: Option[Double],
    consumptionWh: Double,
    stateOfCharge: Option[Double]
)

case class AssetCurrentConsumption(
    currentInstantWatts: Double,
    currentInstantWattsL1: Option[Double],
    currentInstantWattsL2: Option[Double],
    currentInstantWattsL3: Option[Double],
    currentInstantAmps: Option[Double],
    currentInstantAmpsL1: Option[Double],
    currentInstantAmpsL2: Option[Double],
    currentInstantAmpsL3: Option[Double],
    currentInstantVolts: Option[Double],
    currentInstantVoltsL1: Option[Double],
    currentInstantVoltsL2: Option[Double],
    currentInstantVoltsL3: Option[Double],
    currentConsumptionWh: Option[Double],
    currentTotalConsumptionWh: Double,
    currentStateOfCharge: Option[Double],
    lastConsumptionTimestamp: Option[Instant]
)

case class Asset(
    id: AssetId,
    tenantId: TenantId,
    name: String,
    companyId: Option[CompanyId],
    siteId: Option[SiteId],
    siteAreaId: SiteAreaId,
    assetType: AssetType,
    fluctuationPercent: Double,
    staticValueWatt: Double,
    coordinates: List[Double],
    issuer: Boolean,
    image: Option[String],
    dynamicAsset: Boolean,
    usesPushApi: Boolean,
    connectionId: Option[String],
    meterId: Option[String],
    excludeFromSmartCharging: Boolean,
    variationThresholdPercent: Option[Double],
    powerWattsLastSmartCharging: Option[Double],
    currentConsumption: AssetCurrentConsumption,
    createdOn: Instant,
    lastChangedOn: Instant
)

object Asset:
  given Schema[AssetType]               = DeriveSchema.gen
  given Schema[AssetConnectorType]      = DeriveSchema.gen
  given Schema[AssetMeasurement]        = DeriveSchema.gen
  given Schema[AssetCurrentConsumption] = DeriveSchema.gen
  given Schema[Asset]                   = DeriveSchema.gen
