package io.itbeans.ev.kafka

import io.circe.Decoder
import io.circe.parser.decode
import zio._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// EvKafkaConsumer — typed ZIO Kafka consumer wrapper.
//
// Two consumer group strategies:
//
//   competing  — all instances share one group ID (load-balanced; only one pod
//                processes each message).  Default for most workers.
//
//   fanOut     — each instance uses a unique group ID so every pod receives
//                every message.  Used for local-state consumers like the OCPP
//                cross-pod relay response listener.
// ---------------------------------------------------------------------------

trait EvKafkaConsumer:
  /**
   * Consume a single topic with the configured (shared) group ID.
   * Messages are decoded from JSON; decode errors are logged and skipped.
   */
  def consume[A: Decoder](topic: String)(handler: (String, A) => Task[Unit]): Task[Unit]

  /**
   * Consume using a dedicated per-call group ID (fan-out pattern).
   * Each consumer instance in a pod receives all messages independently.
   */
  def consumeFanOut[A: Decoder](
      topic: String,
      uniqueGroupId: String
  )(handler: (String, A) => Task[Unit]): Task[Unit]

  /**
   * Subscribe to topics matching a regex pattern with the shared group ID.
   * Useful for wildcard per-tenant topic subscriptions (e.g. `ocpp.commands.*`).
   */
  def consumePattern[A: Decoder](
      pattern: String
  )(handler: (String, A) => Task[Unit]): Task[Unit]

object EvKafkaConsumer:

  def consume[A: Decoder](topic: String)(handler: (String, A) => Task[Unit]): RIO[EvKafkaConsumer, Unit] =
    ZIO.serviceWithZIO[EvKafkaConsumer](_.consume(topic)(handler))

  def consumeFanOut[A: Decoder](topic: String, uniqueGroupId: String)(
      handler: (String, A) => Task[Unit]
  ): RIO[EvKafkaConsumer, Unit] =
    ZIO.serviceWithZIO[EvKafkaConsumer](_.consumeFanOut(topic, uniqueGroupId)(handler))

  def consumePattern[A: Decoder](pattern: String)(
      handler: (String, A) => Task[Unit]
  ): RIO[EvKafkaConsumer, Unit] =
    ZIO.serviceWithZIO[EvKafkaConsumer](_.consumePattern(pattern)(handler))

final class LiveEvKafkaConsumer(config: KafkaConfig) extends EvKafkaConsumer:

  private def makeSettings(groupId: String): ConsumerSettings =
    ConsumerSettings(config.bootstrapServers)
      .withGroupId(groupId)
      .withProperty("auto.offset.reset", config.autoOffsetReset)
      .withProperty("enable.auto.commit", "false")

  private def runStream[A: Decoder](
      subscription: Subscription,
      groupId: String
  )(handler: (String, A) => Task[Unit]): Task[Unit] =
    ZIO.scoped {
      Consumer
        .make(makeSettings(groupId))
        .flatMap { consumer =>
          consumer
            .plainStream(subscription, Serde.string, Serde.string)
            .mapZIO { record =>
              val key   = record.key
              val value = record.value
              decode[A](value) match
                case Right(event) =>
                  handler(key, event)
                    .catchAll(err => ZIO.logError(s"Handler failed for key=$key: ${err.getMessage}"))
                case Left(err) =>
                  ZIO.logWarning(
                    s"Decode failed for key=$key topic=${record.record.topic}: ${err.getMessage}"
                  )
            }
            .runDrain
        }
    }

  override def consume[A: Decoder](topic: String)(handler: (String, A) => Task[Unit]): Task[Unit] =
    runStream(Subscription.topics(topic), config.groupId)(handler)

  override def consumeFanOut[A: Decoder](
      topic: String,
      uniqueGroupId: String
  )(handler: (String, A) => Task[Unit]): Task[Unit] =
    runStream(Subscription.topics(topic), uniqueGroupId)(handler)

  override def consumePattern[A: Decoder](
      pattern: String
  )(handler: (String, A) => Task[Unit]): Task[Unit] =
    runStream(Subscription.pattern(java.util.regex.Pattern.compile(pattern)), config.groupId)(handler)

object LiveEvKafkaConsumer:

  val live: ZLayer[KafkaConfig, Nothing, EvKafkaConsumer] =
    ZLayer.fromFunction(LiveEvKafkaConsumer(_))
