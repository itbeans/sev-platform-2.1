package io.itbeans.ev.pricingservice

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// Tapir Schema instances for custom types that lack built-in Tapir support
private given Schema[Instant] = Schema.string[Instant]
private given Schema[PricingEntity] = Schema.string[PricingEntity]

private given Schema[DayOfWeek] = Schema.schemaForInt.map(DayOfWeek.fromIso)(_.isoValue)

// ---------------------------------------------------------------------------
// PricingEndpoints — Tapir endpoint definitions for /api/pricing.
//
// All endpoints require X-Tenant-ID header (validated upstream by Kong).
//
//   GET    /api/pricing            — list tariff definitions
//   POST   /api/pricing            — create tariff definition
//   GET    /api/pricing/:id        — get single tariff definition
//   PUT    /api/pricing/:id        — update tariff definition
//   DELETE /api/pricing/:id        — delete tariff definition
// ---------------------------------------------------------------------------

// ── JSON request/response models ──────────────────────────────────────────

case class PricingListResponse(count: Long, result: List[PricingDefinition])

object PricingListResponse:
  given Encoder[PricingListResponse] = deriveEncoder
  given Decoder[PricingListResponse] = deriveDecoder

case class CreatePricingBody(
    entityId: String,
    entityType: PricingEntity,
    name: String,
    description: Option[String],
    siteId: Option[String],
    dimensions: PricingDimensions,
    staticRestrictions: Option[PricingStaticRestriction],
    restrictions: Option[PricingRestriction],
    currencyCode: String
)

object CreatePricingBody:
  given Encoder[CreatePricingBody] = deriveEncoder
  given Decoder[CreatePricingBody] = deriveDecoder

case class UpdatePricingBody(
    name: Option[String],
    description: Option[String],
    dimensions: Option[PricingDimensions],
    staticRestrictions: Option[PricingStaticRestriction],
    restrictions: Option[PricingRestriction],
    currencyCode: Option[String]
)

object UpdatePricingBody:
  given Encoder[UpdatePricingBody] = deriveEncoder
  given Decoder[UpdatePricingBody] = deriveDecoder

// ── Base endpoint ──────────────────────────────────────────────────────────

private val tenantHeader = header[String]("X-Tenant-ID")

private val base =
  endpoint
    .in("api" / "pricing")
    .errorOut(stringBody)

// ── Endpoint definitions ───────────────────────────────────────────────────

object PricingEndpoints:

  val listPricing =
    base.get
      .in(tenantHeader)
      .in(query[Option[Int]]("Limit"))
      .in(query[Option[Int]]("Skip"))
      .out(jsonBody[PricingListResponse])

  val createPricing =
    base.post
      .in(tenantHeader)
      .in(jsonBody[CreatePricingBody])
      .out(jsonBody[PricingDefinition])

  val getPricing =
    base.get
      .in(path[String]("id"))
      .in(tenantHeader)
      .out(jsonBody[PricingDefinition])

  val updatePricing =
    base.put
      .in(path[String]("id"))
      .in(tenantHeader)
      .in(jsonBody[UpdatePricingBody])
      .out(jsonBody[PricingDefinition])

  val deletePricing =
    base.delete
      .in(path[String]("id"))
      .in(tenantHeader)
      .out(emptyOutput)
