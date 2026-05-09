package io.itbeans.ev.billingservice

import io.circe.parser.decode
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import zio._
import zio.kafka.consumer._
import zio.kafka.serde.Serde

// ---------------------------------------------------------------------------
// BillingKafkaConsumer — subscribes to transactions.lifecycle.
//
// Filters for action == "Stop" with a userId present (billable sessions).
// Creates a Stripe draft invoice for each completed billable session.
//
// Offset commit strategy: at-least-once — offset committed after processing.
// Failed invoice creation is logged but does not block the offset commit to
// avoid poisoning the consumer partition on a bad message.
// ---------------------------------------------------------------------------

trait BillingKafkaConsumer:
  def start: Task[Unit]

final class LiveBillingKafkaConsumer(
    billing: BillingService,
    kafkaCfg: KafkaConfig
) extends BillingKafkaConsumer:

  def start: Task[Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId("ev-billing-transactions")
      .withProperty("auto.offset.reset", "earliest")

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(Subscription.topics(Topics.transactionLifecycle), Serde.string, Serde.string)
          .mapZIO { record =>
            val process = decode[TransactionLifecycleBillingPayload](record.value) match
              case Left(err) =>
                ZIO.logWarning(s"Billing: failed to decode transaction lifecycle payload: $err")
              case Right(payload) if payload.action != "Stop" =>
                ZIO.unit // only process Stop events
              case Right(payload) if payload.userId.isEmpty =>
                ZIO.logDebug(s"Billing: tx ${payload.transactionId} has no userId — not billable")
              case Right(payload) =>
                billing
                  .createInvoiceForSession(payload.tenantId, payload)
                  .tapError(err =>
                    ZIO.logError(
                      s"Billing: invoice creation failed for tx ${payload.transactionId}: ${err.getMessage}"
                    )
                  )
                  .ignore

            process.as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

object LiveBillingKafkaConsumer:

  val live: ZLayer[BillingService & KafkaConfig, Nothing, BillingKafkaConsumer] =
    ZLayer.fromFunction(new LiveBillingKafkaConsumer(_, _))
