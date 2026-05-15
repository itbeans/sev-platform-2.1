package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type ConnectionId = String

object ConnectionId:
  def apply(v: String): ConnectionId             = v
  def unapply(id: ConnectionId): Some[String]    = Some(id)
  extension (id: ConnectionId) def value: String = id
  given Schema[ConnectionId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(ConnectionId(_), _.value)

// ---------------------------------------------------------------------------
// Connection domain — external data-source connections used by the asset service
// to pull real-time telemetry from power meters and energy management systems.
//
// Each Connection belongs to a tenant and is referenced by Assets via connectionId.
// Mirrors the TypeScript Connection / AssetConnection storage model.
// ---------------------------------------------------------------------------

// Supported external connector vendors
enum ExternalConnectorType(val code: String):
  case Greencom  extends ExternalConnectorType("greencom")
  case IOThink   extends ExternalConnectorType("iothink")
  case Lacroix   extends ExternalConnectorType("lacroix")
  case Schneider extends ExternalConnectorType("schneider")
  case Wit       extends ExternalConnectorType("wit")

enum ConnectionStatus:
  case Active, Inactive, Error

// Credentials and endpoint configuration for one external integration
case class Connection(
    id: ConnectionId,
    tenantId: TenantId,
    name: String,
    connectorType: ExternalConnectorType,
    url: String,
    refreshIntervalMins: Int,
    status: ConnectionStatus,
    // OAuth / API-key auth (fields present depend on connectorType)
    clientId: Option[String],
    clientSecret: Option[String],
    authenticationUrl: Option[String],
    user: Option[String],
    password: Option[String],
    lastSuccessAt: Option[Instant],
    lastErrorAt: Option[Instant],
    lastError: Option[String],
    createdOn: Instant,
    lastChangedOn: Instant
)

object Connection:
  given Schema[ExternalConnectorType] = DeriveSchema.gen
  given Schema[ConnectionStatus]      = DeriveSchema.gen
  given Schema[Connection]            = DeriveSchema.gen
