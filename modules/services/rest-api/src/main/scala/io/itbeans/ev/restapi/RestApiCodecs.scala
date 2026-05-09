package io.itbeans.ev.restapi

import io.circe.{Decoder, Encoder}
import io.itbeans.ev.domain._
import sttp.tapir.{Schema => TSchema}

import java.time.Instant

// ---------------------------------------------------------------------------
// RestApiCodecs — Circe and Tapir codecs for opaque domain types.
//
// Opaque types in Scala 3 are invisible to Circe's generic auto-derivation
// and Tapir's generic schema derivation. These explicit instances bridge
// the gap so endpoint files can use domain types directly in jsonBody[T].
//
// Usage in endpoint files:
//   import RestApiCodecs.given
// ---------------------------------------------------------------------------

object RestApiCodecs:

  // ── java.time.Instant ────────────────────────────────────────────────────

  given Encoder[Instant] = Encoder[Long].contramap(_.toEpochMilli)
  given Decoder[Instant] = Decoder[Long].map(Instant.ofEpochMilli)

  given TSchema[Instant] =
    TSchema.schemaForLong.map(l => Some(Instant.ofEpochMilli(l)))(_.toEpochMilli)

  // ── Opaque ID types ───────────────────────────────────────────────────────

  given Encoder[ChargingStationId] = Encoder[String].contramap(_.value)
  given Decoder[ChargingStationId] = Decoder[String].map(ChargingStationId(_))

  given TSchema[ChargingStationId] =
    TSchema.schemaForString.map(s => Some(ChargingStationId(s)))(_.value)

  given Encoder[TenantId] = Encoder[String].contramap(_.value)
  given Decoder[TenantId] = Decoder[String].map(TenantId(_))

  given TSchema[TenantId] =
    TSchema.schemaForString.map(s => Some(TenantId(s)))(_.value)

  given Encoder[SiteId] = Encoder[String].contramap(_.value)
  given Decoder[SiteId] = Decoder[String].map(SiteId(_))

  given TSchema[SiteId] =
    TSchema.schemaForString.map(s => Some(SiteId(s)))(_.value)

  given Encoder[SiteAreaId] = Encoder[String].contramap(_.value)
  given Decoder[SiteAreaId] = Decoder[String].map(SiteAreaId(_))

  given TSchema[SiteAreaId] =
    TSchema.schemaForString.map(s => Some(SiteAreaId(s)))(_.value)

  given Encoder[CompanyId] = Encoder[String].contramap(_.value)
  given Decoder[CompanyId] = Decoder[String].map(CompanyId(_))

  given TSchema[CompanyId] =
    TSchema.schemaForString.map(s => Some(CompanyId(s)))(_.value)

  given Encoder[UserId] = Encoder[String].contramap(_.value)
  given Decoder[UserId] = Decoder[String].map(UserId(_))

  given TSchema[UserId] =
    TSchema.schemaForString.map(s => Some(UserId(s)))(_.value)

  given Encoder[TransactionId] = Encoder[Long].contramap(_.value)
  given Decoder[TransactionId] = Decoder[Long].map(TransactionId(_))

  given TSchema[TransactionId] =
    TSchema.schemaForLong.map(l => Some(TransactionId(l)))(_.value)
