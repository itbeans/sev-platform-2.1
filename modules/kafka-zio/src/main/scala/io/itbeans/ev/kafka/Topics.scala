package io.itbeans.ev.kafka

import io.itbeans.ev.domain.TenantId

// ---------------------------------------------------------------------------
// Kafka topic definitions — single source of truth for all service topic names.
//
// Partitioning strategy:
//   - ocpp.events.{tenantId}      → partitioned by chargingStationId (sequential per station)
//   - ocpp.commands.{tenantId}    → partitioned by chargingStationId
//   - transactions.lifecycle      → partitioned by transactionId
//   - all others                  → default partitioning
// ---------------------------------------------------------------------------

object Topics:

  // ── Per-tenant topics (one Kafka topic per tenant) ────────────────────────

  /** All OCPP station-originated messages from the Gateway to the Processor. */
  def ocppEvents(tenantId: TenantId): String =
    s"ocpp.events.${tenantId.value}"

  /** Commands from CSMS to station (routed via Gateway WebSocket). */
  def ocppCommands(tenantId: TenantId): String =
    s"ocpp.commands.${tenantId.value}"

  // ── Global topics (shared across all tenants, tenantId in message payload) ─

  /**
   * Transaction start / update / stop events.
   *  Consumers: Billing, Roaming, Analytics, Notification, Smart Charging.
   */
  val transactionLifecycle: String = "transactions.lifecycle"

  /** Smart charging computation triggers (debounced per siteAreaId). */
  val smartChargingTriggers: String = "smart-charging.triggers"

  /** Outbound notification requests (email, push). */
  val notificationsOutbound: String = "notifications.outbound"

  /** Billing async task queue (replaces MongoDB asynctasks change stream). */
  val billingAsyncTasks: String = "billing.async-tasks"

  /** Real-time asset consumption events (solar, battery, building loads). */
  val assetConsumptions: String = "asset.consumptions"

  /** Audit log events from all services → Analytics service. */
  val auditLog: String = "audit.log"

  /** Consumption readings from MeterValues → ev-analytics TimescaleDB hypertable. */
  val consumptionsIngest: String = "consumptions.ingest"

  /** Dead letter queue — messages that failed processing after retries. */
  val deadLetterQueue: String = "dlq.failed-messages"

  /** Cross-pod OCPP command relay responses, keyed by correlationId. */
  val ocppCommandResponses: String = "ocpp.command_responses"
