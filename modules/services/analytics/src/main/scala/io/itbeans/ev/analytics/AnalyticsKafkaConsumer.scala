package io.itbeans.ev.analytics

import io.circe.parser.decode
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import zio._
import zio.kafka.consumer._
import zio.kafka.serde.Serde

import java.time.Instant

// ---------------------------------------------------------------------------
// AnalyticsKafkaConsumer — writes analytics data into TimescaleDB hypertables.
//
// Topics consumed:
//   audit.log           → ev_logs hypertable
//
// Offset commit: at-least-once (INSERT … ON CONFLICT DO NOTHING makes
// duplicate inserts from retries safe for consumptions; log inserts are
// idempotent by (tenant_id, time, action) in practice).
// ---------------------------------------------------------------------------

trait AnalyticsKafkaConsumer:
  def start: Task[Unit]

final class LiveAnalyticsKafkaConsumer(
    logRepo: LogRepository,
    kafkaCfg: KafkaConfig
) extends AnalyticsKafkaConsumer:

  def start: Task[Unit] =
    startAuditLogConsumer

  // ── audit.log consumer ────────────────────────────────────────────────

  private def startAuditLogConsumer: Task[Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId("ev-analytics-audit-log")
      .withProperty("auto.offset.reset", "earliest")

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(Subscription.topics(Topics.auditLog), Serde.string, Serde.string)
          .mapZIO { record =>
            val process = decode[AuditLogPayload](record.value) match
              case Left(err) =>
                ZIO.logWarning(s"[Analytics] Failed to decode AuditLogPayload: $err")
              case Right(payload) =>
                val entry = LogEntry(
                  tenantId = payload.tenantId,
                  time = Instant.ofEpochMilli(payload.timestamp),
                  level = payload.level,
                  source = payload.source,
                  action = payload.action,
                  module = payload.module,
                  method = payload.method,
                  message = payload.message,
                  userId = payload.userId,
                  chargingStationId = payload.chargingStationId,
                  siteId = payload.siteId,
                  siteAreaId = payload.siteAreaId,
                  details = payload.detailedMessages
                )
                logRepo.insert(entry)
                  .tapError(err =>
                    ZIO.logError(s"[Analytics] Failed to insert log entry: ${err.getMessage}")
                  )
                  .ignore

            process.as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

object LiveAnalyticsKafkaConsumer:

  val live: ZLayer[LogRepository & KafkaConfig, Nothing, AnalyticsKafkaConsumer] =
    ZLayer.fromFunction(new LiveAnalyticsKafkaConsumer(_, _))
