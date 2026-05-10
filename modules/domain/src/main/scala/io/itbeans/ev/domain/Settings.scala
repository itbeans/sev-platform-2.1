package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type RegistrationTokenId = String

object RegistrationTokenId:
  def apply(v: String): RegistrationTokenId             = v
  def unapply(id: RegistrationTokenId): Some[String]    = Some(id)
  extension (id: RegistrationTokenId) def value: String = id
  given Schema[RegistrationTokenId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(RegistrationTokenId(_), _.value)

// ---------------------------------------------------------------------------
// Settings domain — per-tenant configuration, registration tokens, OCPP endpoint
// settings, and component-level feature flags.
// Mirrors the TypeScript Settings / RegistrationToken storage model.
// ---------------------------------------------------------------------------

// Token used by a charging station to register with the platform (boot handshake)
case class RegistrationToken(
    id: RegistrationTokenId,
    tenantId: TenantId,
    description: String,
    expirationDate: Option[Instant],
    revocationDate: Option[Instant],
    siteAreaId: Option[SiteAreaId],
    revoked: Boolean = false,
    createdOn: Instant,
    lastChangedOn: Instant
)

// OCPP endpoint configuration stored in tenant settings
case class OcppEndpointSettings(
    localPreAuthorize: Boolean,
    heartbeatIntervalSecs: Int,
    connectionTimeoutSecs: Int,
    maxStartTransactionRetries: Int,
    autoReconnectMaxRetries: Int
)

// Smart charging component configuration
case class SmartChargingSettings(
    enabled: Boolean,
    optimizerUrl: Option[String],
    optimizerApiKey: Option[String],
    limitEnabled: Boolean
)

// Billing component configuration (Stripe keys, etc.)
case class BillingSettings(
    enabled: Boolean,
    immediateBillingAllowed: Boolean,
    periodicBillingAllowed: Boolean,
    taxId: Option[String],
    stripeSecretKey: Option[String],
    stripePublicKey: Option[String]
)

// Analytics component configuration (TimescaleDB / Grafana)
case class AnalyticsSettings(
    enabled: Boolean,
    adminUrl: Option[String],
    adminPassword: Option[String]
)

// Top-level per-tenant settings document
case class TenantSettings(
    id: String,
    tenantId: TenantId,
    ocppEndpoint: Option[OcppEndpointSettings],
    smartCharging: Option[SmartChargingSettings],
    billing: Option[BillingSettings],
    analytics: Option[AnalyticsSettings],
    createdOn: Instant,
    lastChangedOn: Instant
)

object Settings:
  given Schema[RegistrationToken]      = DeriveSchema.gen
  given Schema[OcppEndpointSettings]   = DeriveSchema.gen
  given Schema[SmartChargingSettings]  = DeriveSchema.gen
  given Schema[BillingSettings]        = DeriveSchema.gen
  given Schema[AnalyticsSettings]      = DeriveSchema.gen
  given Schema[TenantSettings]         = DeriveSchema.gen
