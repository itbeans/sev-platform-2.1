package io.itbeans.ev.notification

import io.circe.parser.decode
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import io.itbeans.ev.notification.NotificationRequest.given
import zio._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde

// ---------------------------------------------------------------------------
// KafkaConsumer — consumes the notifications.outbound topic.
//
// Message format: JSON-serialised NotificationRequest.
// Consumer group: ev-notification (configurable via KafkaConfig.groupId).
// Offset strategy: manual commit after each record to ensure at-least-once
//   delivery semantics — a notification may be sent more than once on restart,
//   but the NotificationStorage dedup layer prevents user-visible duplicates.
// ---------------------------------------------------------------------------

final class KafkaConsumer(
    service: NotificationService,
    config: KafkaConfig
):

  def start: Task[Unit] =
    val settings = ConsumerSettings(config.bootstrapServers)
      .withGroupId(config.groupId)
      .withProperty("auto.offset.reset", config.autoOffsetReset)
      .withProperty("enable.auto.commit", "false")

    ZIO.scoped {
      Consumer
        .make(settings)
        .flatMap { consumer =>
          consumer
            .plainStream(
              Subscription.topics(Topics.notificationsOutbound),
              Serde.string,
              Serde.string
            )
            .mapZIO { record =>
              processRecord(record.value)
                .catchAll(err =>
                  ZIO.logError(s"Failed to process notification record: $err")
                )
                .as(record.offset)
            }
            .aggregateAsync(Consumer.offsetBatches)
            .mapZIO(_.commit)
            .runDrain
        }
    }

  private def processRecord(json: String): Task[Unit] =
    for
      req <- ZIO
        .fromEither(decode[NotificationRequest](json))
        .mapError(err =>
          new Exception(s"Invalid NotificationRequest JSON: $err — payload: ${json.take(200)}")
        )
      _ <- service.dispatch(req)
    yield ()

object KafkaConsumer:

  val live: ZLayer[NotificationService & KafkaConfig, Nothing, KafkaConsumer] =
    ZLayer.fromFunction(new KafkaConsumer(_, _))
