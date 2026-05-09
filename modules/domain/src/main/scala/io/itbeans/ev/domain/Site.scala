package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}

opaque type SiteId     = String
opaque type SiteAreaId = String
opaque type CompanyId  = String

object SiteId:
  def apply(v: String): SiteId             = v
  def unapply(id: SiteId): Some[String]    = Some(id)
  extension (id: SiteId) def value: String = id
  given Schema[SiteId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(SiteId(_), _.value)

object SiteAreaId:
  def apply(v: String): SiteAreaId             = v
  def unapply(id: SiteAreaId): Some[String]    = Some(id)
  extension (id: SiteAreaId) def value: String = id

  given Schema[SiteAreaId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(SiteAreaId(_), _.value)

object CompanyId:
  def apply(v: String): CompanyId             = v
  def unapply(id: CompanyId): Some[String]    = Some(id)
  extension (id: CompanyId) def value: String = id
  given Schema[CompanyId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(CompanyId(_), _.value)

case class Site(
    id: SiteId,
    tenantId: TenantId,
    companyId: CompanyId,
    name: String,
    address: Address,
    coordinates: Option[Coordinates],
    public: Boolean,
    autoUserSiteAssignment: Boolean,
    userIds: List[UserId],
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

case class SiteArea(
    id: SiteAreaId,
    tenantId: TenantId,
    siteId: SiteId,
    name: String,
    maximumPower: Double,
    voltage: Option[Int],
    numberOfPhases: Option[Int],
    address: Option[Address],
    coordinates: Option[Coordinates],
    smartCharging: Boolean,
    accessControl: Boolean,
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

case class Company(
    id: CompanyId,
    tenantId: TenantId,
    name: String,
    address: Option[Address],
    logo: Option[String],
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

object Site:
  given Schema[Site] = DeriveSchema.gen
  given Schema[SiteArea] = DeriveSchema.gen
  given Schema[Company] = DeriveSchema.gen
