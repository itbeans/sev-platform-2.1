package io.itbeans.ev.asset

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// Tapir Schema instances for custom types
private given Schema[Instant] = Schema.string[Instant]
private given Schema[AssetType] = Schema.string[AssetType]
private given Schema[AssetConnectorType] = Schema.string[AssetConnectorType]

// ---------------------------------------------------------------------------
// AssetEndpoints — Tapir endpoint definitions for /api/assets.
//
// All endpoints require X-Tenant-ID header.
//
//   GET    /api/assets             — list assets
//   POST   /api/assets             — create asset
//   GET    /api/assets/:id         — get single asset
//   PUT    /api/assets/:id         — update asset
//   DELETE /api/assets/:id         — delete asset
//   POST   /api/assets/:id/consumptions — push consumption (manual / push API)
// ---------------------------------------------------------------------------

// ── Request/response models ───────────────────────────────────────────────

case class AssetListResponse(count: Long, result: List[Asset])

object AssetListResponse:
  given Encoder[AssetListResponse] = deriveEncoder
  given Decoder[AssetListResponse] = deriveDecoder

case class CreateAssetBody(
    name: String,
    companyId: String,
    siteId: Option[String],
    siteAreaId: String,
    assetType: AssetType,
    fluctuationPercent: Double,
    staticValueWatt: Double,
    coordinates: List[Double],
    issuer: Boolean,
    dynamicAsset: Boolean,
    usesPushApi: Boolean,
    connectionId: Option[String],
    meterId: Option[String],
    excludeFromSmartCharging: Boolean,
    variationThresholdPercent: Option[Double]
)

object CreateAssetBody:
  given Encoder[CreateAssetBody] = deriveEncoder
  given Decoder[CreateAssetBody] = deriveDecoder

case class UpdateAssetBody(
    name: Option[String],
    assetType: Option[AssetType],
    fluctuationPercent: Option[Double],
    staticValueWatt: Option[Double],
    coordinates: Option[List[Double]],
    dynamicAsset: Option[Boolean],
    usesPushApi: Option[Boolean],
    connectionId: Option[String],
    meterId: Option[String],
    excludeFromSmartCharging: Option[Boolean],
    variationThresholdPercent: Option[Double]
)

object UpdateAssetBody:
  given Encoder[UpdateAssetBody] = deriveEncoder
  given Decoder[UpdateAssetBody] = deriveDecoder

case class PushConsumptionBody(
    timestamp: Instant,
    instantWatts: Double,
    consumptionWh: Double,
    stateOfCharge: Option[Double]
)

object PushConsumptionBody:
  given Encoder[PushConsumptionBody] = deriveEncoder
  given Decoder[PushConsumptionBody] = deriveDecoder

// ── Base endpoint ──────────────────────────────────────────────────────────

private val tenantHeader = header[String]("X-Tenant-ID")

private val base =
  endpoint
    .in("api" / "assets")
    .errorOut(stringBody)

// ── Endpoints ─────────────────────────────────────────────────────────────

object AssetEndpoints:

  val listAssets =
    base.get
      .in(tenantHeader)
      .in(query[Option[Boolean]]("DynamicOnly"))
      .in(query[Option[Int]]("Limit"))
      .in(query[Option[Int]]("Skip"))
      .out(jsonBody[AssetListResponse])

  val createAsset =
    base.post
      .in(tenantHeader)
      .in(jsonBody[CreateAssetBody])
      .out(jsonBody[Asset])

  val getAsset =
    base.get
      .in(path[String]("id"))
      .in(tenantHeader)
      .out(jsonBody[Asset])

  val updateAsset =
    base.put
      .in(path[String]("id"))
      .in(tenantHeader)
      .in(jsonBody[UpdateAssetBody])
      .out(jsonBody[Asset])

  val deleteAsset =
    base.delete
      .in(path[String]("id"))
      .in(tenantHeader)
      .out(emptyOutput)

  val pushConsumption =
    base.post
      .in(path[String]("id") / "consumptions")
      .in(tenantHeader)
      .in(jsonBody[PushConsumptionBody])
      .out(emptyOutput)
