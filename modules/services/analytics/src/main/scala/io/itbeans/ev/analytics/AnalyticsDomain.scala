package io.itbeans.ev.analytics

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

import java.time.Instant

// ---------------------------------------------------------------------------
// ev-analytics domain types
//
// Statistics: aggregated views over ev_consumptions hypertable.
// Logs:       audit log entries from ev_logs hypertable.
// Kafka:      AuditLogEntry consumed from audit.log topic.
// ---------------------------------------------------------------------------

// ── Statistics ────────────────────────────────────────────────────────────

/** One data point in a statistics series. */
case class StatPoint(
    label: String,  // e.g. charging station ID or user ID
    period: String, // e.g. "2024-01" for monthly, "2024-01-15" for daily
    value: Double
)

object StatPoint:
  given Encoder[StatPoint] = deriveEncoder
  given Decoder[StatPoint] = deriveDecoder

case class StatisticsResponse(
    dataType: String,  // "Consumption" | "Usage" | "Inactivity" | "Transactions" | "Pricing"
    dataScope: String, // "year" | "month" | "day"
    unit: String,      // "kWh" | "h" | "" | "€"
    total: Double,
    data: List[StatPoint]
)

object StatisticsResponse:
  given Encoder[StatisticsResponse] = deriveEncoder
  given Decoder[StatisticsResponse] = deriveDecoder

// ── Consumption ingestion (from Kafka / OCPP processor) ───────────────────

/** Inbound consumption reading published by ev-ocpp-processor. */
case class ConsumptionReading(
    tenantId: String,
    chargingStationId: String,
    connectorId: Int,
    siteAreaId: Option[String],
    siteId: Option[String],
    userId: Option[String],
    transactionId: Option[Long],
    time: Instant,
    instantWatts: Double,
    cumulatedKwh: Double
)

object ConsumptionReading:
  given Encoder[ConsumptionReading] = deriveEncoder
  given Decoder[ConsumptionReading] = deriveDecoder

// ── Audit log ─────────────────────────────────────────────────────────────

/** An audit log entry as stored in the ev_logs hypertable. */
case class LogEntry(
    tenantId: String,
    time: Instant,
    level: String, // "D" | "I" | "W" | "E"
    source: String,
    action: String,
    module: Option[String],
    method: Option[String],
    message: String,
    userId: Option[String],
    chargingStationId: Option[String],
    siteId: Option[String],
    siteAreaId: Option[String],
    details: Option[String] // JSON blob
)

object LogEntry:
  given Encoder[LogEntry] = deriveEncoder
  given Decoder[LogEntry] = deriveDecoder

/** Pagination envelope for log queries. */
case class LogsResponse(
    count: Int,
    logs: List[LogEntry]
)

object LogsResponse:
  given Encoder[LogsResponse] = deriveEncoder
  given Decoder[LogsResponse] = deriveDecoder

/**
 * Payload published by ev-ocpp-processor to the `consumptions.ingest` Kafka topic
 * on every MeterValues / TransactionEvent.Updated message.
 * `time` is epoch-milliseconds; `siteAreaId` / `siteId` / `userId` are enriched
 * if available in the transaction document.
 */
case class ConsumptionIngestPayload(
    tenantId: String,
    chargingStationId: String,
    connectorId: Int,
    transactionId: Option[Long],
    siteAreaId: Option[String],
    siteId: Option[String],
    userId: Option[String],
    time: Long,
    instantWatts: Double,
    cumulatedKwh: Double
)

object ConsumptionIngestPayload:
  given Encoder[ConsumptionIngestPayload] = deriveEncoder
  given Decoder[ConsumptionIngestPayload] = deriveDecoder

/** Payload consumed from the Kafka `audit.log` topic. */
case class AuditLogPayload(
    tenantId: String,
    timestamp: Long, // epoch millis
    level: String,
    source: String,
    action: String,
    module: Option[String],
    method: Option[String],
    message: String,
    userId: Option[String],
    chargingStationId: Option[String],
    siteId: Option[String],
    siteAreaId: Option[String],
    detailedMessages: Option[String]
)

object AuditLogPayload:
  given Encoder[AuditLogPayload] = deriveEncoder
  given Decoder[AuditLogPayload] = deriveDecoder
