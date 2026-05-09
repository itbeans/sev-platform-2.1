package io.itbeans.ev.car

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import io.itbeans.ev.domain.UserId
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// Tapir Schema instances for custom types (required by jsonBody[T] derivation)
private given Schema[Instant] = Schema.string[Instant]
private given Schema[UserId] = Schema.string[UserId]
private given Schema[CarType] = Schema.string[CarType]
private given Schema[ConverterType] = Schema.string[ConverterType]

// ---------------------------------------------------------------------------
// CarEndpoints — Tapir endpoint definitions for /api/cars and /api/carcatalogs.
//
// All endpoints require X-Tenant-ID header (validated upstream by Kong).
// Authorization is handled by Kong JWT plugin; the service trusts the header.
//
// Endpoints (mirrors TypeScript CarService.ts REST handlers):
//
//   Car Catalogs:
//     GET  /api/carcatalogs            — list with search/maker filter
//     GET  /api/carcatalogs/:id        — single catalog
//     GET  /api/carcatalogs/:id/image  — thumbnail image (public)
//     GET  /api/carmakers              — distinct makes
//     POST /api/carcatalogs/sync       — trigger sync (admin only)
//
//   Cars:
//     GET    /api/cars                 — list (filtered by userID, search)
//     POST   /api/cars                 — create
//     GET    /api/cars/:id             — single car
//     PUT    /api/cars/:id             — update
//     DELETE /api/cars/:id             — delete
//     GET    /api/cars/:id/soc         — live SoC from car connector
// ---------------------------------------------------------------------------

// ── JSON request/response models ──────────────────────────────────────────

case class CarListResponse(count: Long, result: List[Car])

object CarListResponse:
  given Encoder[CarListResponse] = deriveEncoder
  given Decoder[CarListResponse] = deriveDecoder

case class CatalogListResponse(count: Long, result: List[CarCatalogSummary])

object CatalogListResponse:
  given Encoder[CatalogListResponse] = deriveEncoder
  given Decoder[CatalogListResponse] = deriveDecoder

case class CreateCarBody(
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Int,
    userId: String,
    carType: CarType,
    default: Boolean,
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData]
)

object CreateCarBody:
  given Encoder[CreateCarBody] = deriveEncoder
  given Decoder[CreateCarBody] = deriveDecoder

case class UpdateCarBody(
    vin: Option[String],
    licensePlate: Option[String],
    carCatalogId: Option[Int],
    userId: Option[String],
    carType: Option[CarType],
    default: Option[Boolean],
    converter: Option[CarConverter],
    carConnectorData: Option[CarConnectorData]
)

object UpdateCarBody:
  given Encoder[UpdateCarBody] = deriveEncoder
  given Decoder[UpdateCarBody] = deriveDecoder

case class SyncResponse(inSuccess: Int, inError: Int)

object SyncResponse:
  given Encoder[SyncResponse] = deriveEncoder
  given Decoder[SyncResponse] = deriveDecoder

case class SoCResponse(found: Boolean, soc: Option[Int])

object SoCResponse:
  given Encoder[SoCResponse] = deriveEncoder
  given Decoder[SoCResponse] = deriveDecoder

case class MakersResponse(result: List[String])

object MakersResponse:
  given Encoder[MakersResponse] = deriveEncoder
  given Decoder[MakersResponse] = deriveDecoder

case class ImageResponse(image: Option[String])

object ImageResponse:
  given Encoder[ImageResponse] = deriveEncoder
  given Decoder[ImageResponse] = deriveDecoder

// ── Base endpoint with tenant header ──────────────────────────────────────

private val tenantHeader = header[String]("X-Tenant-ID")

private val base =
  endpoint
    .in("api")
    .errorOut(stringBody)

// ── Car Catalog endpoints ─────────────────────────────────────────────────

object CatalogEndpoints:

  val listCatalogs =
    base.get
      .in("carcatalogs")
      .in(tenantHeader)
      .in(query[Option[String]]("Search"))
      .in(query[Option[String]]("CarMaker"))
      .in(query[Option[Int]]("Limit"))
      .in(query[Option[Int]]("Skip"))
      .out(jsonBody[CatalogListResponse])

  val getCatalog =
    base.get
      .in("carcatalogs" / path[Int]("id"))
      .in(tenantHeader)
      .out(jsonBody[CarCatalog])

  val getCatalogImage =
    base.get
      .in("carcatalogs" / path[Int]("id") / "image")
      .out(jsonBody[ImageResponse])

  val getCarMakers =
    base.get
      .in("carmakers")
      .in(tenantHeader)
      .out(jsonBody[MakersResponse])

  val syncCatalogs =
    base.post
      .in("carcatalogs" / "sync")
      .in(tenantHeader)
      .out(jsonBody[SyncResponse])

// ── Car endpoints ─────────────────────────────────────────────────────────

object CarEndpoints:

  val listCars =
    base.get
      .in("cars")
      .in(tenantHeader)
      .in(query[Option[String]]("UserID"))
      .in(query[Option[String]]("Search"))
      .in(query[Option[Int]]("Limit"))
      .in(query[Option[Int]]("Skip"))
      .out(jsonBody[CarListResponse])

  val createCar =
    base.post
      .in("cars")
      .in(tenantHeader)
      .in(jsonBody[CreateCarBody])
      .out(jsonBody[Car])

  val getCar =
    base.get
      .in("cars" / path[String]("id"))
      .in(tenantHeader)
      .out(jsonBody[Car])

  val updateCar =
    base.put
      .in("cars" / path[String]("id"))
      .in(tenantHeader)
      .in(jsonBody[UpdateCarBody])
      .out(jsonBody[Car])

  val deleteCar =
    base.delete
      .in("cars" / path[String]("id"))
      .in(tenantHeader)
      .out(emptyOutput)

  val getCarSoC =
    base.get
      .in("cars" / path[String]("id") / "soc")
      .in(tenantHeader)
      .out(jsonBody[SoCResponse])
