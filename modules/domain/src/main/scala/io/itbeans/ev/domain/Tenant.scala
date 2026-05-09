package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}

// ---------------------------------------------------------------------------
// Tenant — top-level isolation boundary.
// Every MongoDB collection is prefixed: {tenantId}.{collectionName}
// ---------------------------------------------------------------------------

opaque type TenantId = String

object TenantId:
  def apply(value: String): TenantId         = value
  def unapply(id: TenantId): Some[String]    = Some(id)
  extension (id: TenantId) def value: String = id
  given Schema[TenantId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(TenantId(_), _.value)

case class Tenant(
    id: TenantId,
    name: String,
    subdomain: String,
    email: String,
    activeComponents: Set[TenantComponent],
    address: Option[Address],
    logo: Option[String],
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

enum TenantComponent:

  case Pricing, Billing, SmartCharging, Refund, Statistics,
    CarConnector, Asset, OCPI, OICP, Analytics

case class Address(
    address1: String,
    address2: Option[String],
    postalCode: String,
    city: String,
    department: Option[String],
    region: Option[String],
    country: String,
    latitude: Option[Double],
    longitude: Option[Double],
    coordinates: Option[Coordinates]
)

case class Coordinates(latitude: Double, longitude: Double)

object Tenant:
  given Schema[TenantComponent] = DeriveSchema.gen
  given Schema[Coordinates] = DeriveSchema.gen
  given Schema[Address] = DeriveSchema.gen
  given Schema[Tenant] = DeriveSchema.gen
