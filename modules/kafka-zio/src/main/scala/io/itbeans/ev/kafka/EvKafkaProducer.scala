package io.itbeans.ev.kafka

import io.circe.Encoder
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde

// ---------------------------------------------------------------------------
// EvKafkaProducer — typed ZIO Kafka producer with tenant-aware topic routing.
// ---------------------------------------------------------------------------

trait EvKafkaProducer:
  /** Publish an event to a specific topic, keyed by `partitionKey`. */
  def publish[A: Encoder](topic: String, partitionKey: String, event: A): Task[Unit]

  /** Publish to a per-tenant OCPP topic. */
  def publishOcppEvent[A: Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit]

object EvKafkaProducer:

  def publish[A: Encoder](topic: String, key: String, event: A): RIO[EvKafkaProducer, Unit] =
    ZIO.serviceWithZIO[EvKafkaProducer](_.publish(topic, key, event))

  def publishOcppEvent[A: Encoder](tenantId: TenantId, key: String, event: A): RIO[EvKafkaProducer, Unit] =
    ZIO.serviceWithZIO[EvKafkaProducer](_.publishOcppEvent(tenantId, key, event))

final class LiveEvKafkaProducer(producer: Producer) extends EvKafkaProducer:

  override def publish[A: Encoder](topic: String, partitionKey: String, event: A): Task[Unit] =
    val record = new ProducerRecord[String, String](topic, partitionKey, event.asJson.noSpaces)
    producer.produce(record, Serde.string, Serde.string).unit

  override def publishOcppEvent[A: Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
    publish(Topics.ocppEvents(tenantId), partitionKey, event)

object LiveEvKafkaProducer:

  val live: ZLayer[KafkaConfig, Throwable, EvKafkaProducer] =
    ZLayer.scoped {
      for
        config <- ZIO.service[KafkaConfig]
        settings = ProducerSettings(config.bootstrapServers)
          .withProperty("acks", "all")
          .withProperty("enable.idempotence", "true")
          .withProperty("max.in.flight.requests.per.connection", "5")
        producer <- Producer.make(settings)
      yield new LiveEvKafkaProducer(producer)
    }

/** Kafka configuration. */
case class KafkaConfig(
    bootstrapServers: List[String],
    groupId: String,
    autoOffsetReset: String = "earliest"
)

object KafkaConfig:

  given zio.Config[KafkaConfig] =
    (zio.Config.string("bootstrapServers").map(_.split(",").toList.map(_.trim)) zip
      zio.Config.string("groupId") zip
      zio.Config.string("autoOffsetReset").withDefault("earliest"))
      .map { case (servers, groupId, offset) => KafkaConfig(servers, groupId, offset) }
