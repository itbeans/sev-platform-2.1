package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}

opaque type UserId = String

object UserId:
  def apply(v: String): UserId             = v
  def unapply(id: UserId): Some[String]    = Some(id)
  extension (id: UserId) def value: String = id
  given Schema[UserId] = Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(UserId(_), _.value)

case class User(
    id: UserId,
    tenantId: TenantId,
    firstName: String,
    lastName: String,
    email: String,
    passwordHash: String,
    phone: Option[String],
    mobile: Option[String],
    role: UserRole,
    status: UserStatus,
    locale: String,
    timezone: String,
    isFree: Boolean,
    eulaAccepted: Boolean,
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant,
    lastLoginOn: Option[java.time.Instant],
    // ABAC context — embedded in JWT
    sites: List[String],
    sitesAdmin: List[String],
    sitesOwner: List[String]
)

enum UserRole:
  case SuperAdmin, Admin, SiteAdmin, SiteOwner, Basic, Demo

enum UserStatus:
  case Active, Inactive, Blocked, Pending

object User:
  given Schema[UserRole] = DeriveSchema.gen
  given Schema[UserStatus] = DeriveSchema.gen
  given Schema[User] = DeriveSchema.gen
