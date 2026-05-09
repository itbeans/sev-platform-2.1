package io.itbeans.ev.smartcharging

import io.circe.parser.decode
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import zio._
import zio.kafka.consumer._
import zio.kafka.serde.Serde

import java.time.Instant

// ---------------------------------------------------------------------------
// SmartChargingKafkaConsumer — dual consumer with per-siteAreaId debounce.
//
// Topics consumed:
//   1. `smart-charging.triggers`  — published by ev-ocpp-processor / ev-rest-api
//      payload: SmartChargingTriggerPayload
//   2. `asset.consumptions`       — published by ev-asset (solar, battery loads)
//      payload: AssetConsumptionPayload
//      → translated to a "Reoptimize" trigger for the relevant siteArea
//
// Debounce:
//   Rapid-fire triggers for the same siteAreaId (e.g. 10 MeterValues/s) are
//   collapsed to one optimizer call per `cfg.debounceSecs` window.
//   Implementation: ZIO.Ref[Map[siteAreaId, lastTriggerInstant]] — if
//   (now - lastTrigger) < debounceSecs we skip the call.
//
// Offset commit: at-least-once — committed after the optimization attempt.
// ---------------------------------------------------------------------------

trait SmartChargingKafkaConsumer:
  def start: Task[Unit]

final class LiveSmartChargingKafkaConsumer(
    service: SmartChargingService,
    kafkaCfg: KafkaConfig,
    cfg: SmartChargingConfig,
    debounce: Ref[Map[String, Instant]] // created once per instance via companion layer
) extends SmartChargingKafkaConsumer:

  def start: Task[Unit] =
    ZIO.collectAllPar(List(
      startTriggerConsumer(debounce),
      startAssetConsumer(debounce)
    )).unit

  // ── smart-charging.triggers consumer ─────────────────────────────────

  private def startTriggerConsumer(debounce: Ref[Map[String, Instant]]): Task[Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId("ev-smart-charging-triggers")
      .withProperty("auto.offset.reset", "latest") // process only fresh triggers

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(Subscription.topics(Topics.smartChargingTriggers), Serde.string, Serde.string)
          .mapZIO { record =>
            val process = decode[SmartChargingTriggerPayload](record.value) match
              case Left(err) =>
                ZIO.logWarning(s"[SmartCharging] Failed to decode trigger: $err")
              case Right(payload) =>
                debounceAndRun(debounce, payload.tenantId, payload.siteAreaId, payload.event)

            process.as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

  // ── asset.consumptions consumer ───────────────────────────────────────

  private def startAssetConsumer(debounce: Ref[Map[String, Instant]]): Task[Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId("ev-smart-charging-asset")
      .withProperty("auto.offset.reset", "latest")

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(Subscription.topics(Topics.assetConsumptions), Serde.string, Serde.string)
          .mapZIO { record =>
            val process = decode[AssetConsumptionPayload](record.value) match
              case Left(err) =>
                ZIO.logWarning(s"[SmartCharging] Failed to decode asset consumption: $err")
              case Right(payload) =>
                // Asset consumption changes affect grid headroom → reoptimize
                debounceAndRun(debounce, payload.tenantId, payload.siteAreaId, "Reoptimize")

            process.as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

  // ── Debounce logic ────────────────────────────────────────────────────

  /**
   * For a given (tenantId, siteAreaId): skip optimization if the last trigger
   * was within `cfg.debounceSecs` seconds. Otherwise update the timestamp and
   * run the optimization in the background (non-blocking).
   */
  private def debounceAndRun(
      debounce: Ref[Map[String, Instant]],
      tenantId: String,
      siteAreaId: String,
      event: String
  ): Task[Unit] =
    val key = s"$tenantId|$siteAreaId"
    val now = Instant.now()
    debounce.modify { map =>
      map.get(key) match
        case Some(last) if now.getEpochSecond - last.getEpochSecond < cfg.debounceSecs =>
          (false, map) // still inside debounce window — skip
        case _ =>
          (true, map.updated(key, now)) // outside window — proceed
    }.flatMap { shouldRun =>
      if shouldRun then
        service
          .triggerOptimization(tenantId, siteAreaId, event)
          .tapError(err =>
            ZIO.logError(
              s"[SmartCharging] Optimization failed for $siteAreaId ($event): ${err.getMessage}"
            )
          )
          .ignore
          .forkDaemon
          .unit
      else
        ZIO.logDebug(
          s"[SmartCharging] Debouncing trigger for $siteAreaId ($event) — within ${cfg.debounceSecs}s window"
        )
    }

object LiveSmartChargingKafkaConsumer:

  val live: ZLayer[SmartChargingService & KafkaConfig & SmartChargingConfig, Nothing, SmartChargingKafkaConsumer] =
    ZLayer.fromZIO {
      for
        svc      <- ZIO.service[SmartChargingService]
        kafka    <- ZIO.service[KafkaConfig]
        cfg      <- ZIO.service[SmartChargingConfig]
        debounce <- Ref.make(Map.empty[String, java.time.Instant])
      yield new LiveSmartChargingKafkaConsumer(svc, kafka, cfg, debounce)
    }
