package io.itbeans.ev.kafka

import com.dimafeng.testcontainers.KafkaContainer
import io.circe.generic.semiauto.*
import io.circe.{Decoder, Encoder}
import io.itbeans.ev.domain.TenantId
import zio.*
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.test.*

// ---------------------------------------------------------------------------
// Integration tests for EvKafkaProducer and Topics.
//
// A real Kafka broker is started once per suite via TestContainers (Confluent
// cp-kafka image).  Each integration test publishes to a uniquely-named topic
// and consumes with a fresh consumer group (auto.offset.reset=earliest) so
// tests never interfere with each other.
//
// The "Topics" sub-suite contains pure naming tests that run without the
// container but share the same ZIOSpec for simplicity.
//
// Run: sbt "kafkaZio/test"
// ---------------------------------------------------------------------------

/** Holds both the producer-under-test and the bootstrap address for in-test consumers. */
final case class KafkaTestEnv(bootstrapServers: List[String], producer: EvKafkaProducer)

object EvKafkaProducerSpec extends ZIOSpec[KafkaTestEnv]:

  // Start Kafka container once; build a scoped LiveEvKafkaProducer from it.
  override val bootstrap: ZLayer[Any, Any, KafkaTestEnv] =
    ZLayer.scoped {
      for
        container <- ZIO.acquireRelease(
          ZIO.attempt {
            val c = KafkaContainer()
            c.start()
            c
          }
        )(c => ZIO.attempt(c.stop()).ignore)
        servers = List(container.bootstrapServers)
        producer <- Producer.make(
          ProducerSettings(servers)
            .withProperty("acks", "all")
            .withProperty("enable.idempotence", "true")
            .withProperty("max.in.flight.requests.per.connection", "5")
        )
      yield KafkaTestEnv(servers, new LiveEvKafkaProducer(producer))
    }.orDie

  // ── Test event type ────────────────────────────────────────────────────────

  private case class SampleEvent(name: String, value: Double)
  private given Encoder[SampleEvent] = deriveEncoder
  private given Decoder[SampleEvent] = deriveDecoder

  // ── Consumer helper ────────────────────────────────────────────────────────

  /**
   * Consume the first message from `topic` using a brand-new consumer group
   * (so `auto.offset.reset=earliest` guarantees we see messages published
   * before this consumer subscribed).
   */
  // zio-kafka 2.x: Consumer.plainStream is a companion-object service method
  // (ZStream[Consumer, _, _]).  After Consumer.make we provide the instance via ZLayer.
  // Subscription is passed directly as the first arg — no separate subscribe step.
  private def consumeFirst(servers: List[String], topic: String): Task[String] =
    ZIO
      .scoped {
        Consumer
          .make(
            ConsumerSettings(servers)
              .withGroupId(s"it-test-${java.util.UUID.randomUUID()}")
              .withProperty("auto.offset.reset", "earliest")
              .withProperty("enable.auto.commit", "false")
          )
          .flatMap { consumer =>
            val layer = ZLayer.succeed(consumer)
            Consumer
              .plainStream(Subscription.topics(topic), Serde.string, Serde.string)
              .take(1)
              .map(_.record.value())
              .runHead
              .someOrFail(new RuntimeException(s"No message on topic '$topic'"))
              .provide(layer)
          }
      }
      .timeoutFail(new RuntimeException(s"Timed out waiting for message on '$topic'"))(
        30.seconds
      )

  // ── Spec ──────────────────────────────────────────────────────────────────

  override def spec = suite("ev-kafka-zio")(
    // ── Topic naming (pure — no broker interaction) ─────────────────────────

    suite("Topics")(
      test("ocppEvents embeds the tenantId in the topic name") {
        assertTrue(Topics.ocppEvents(TenantId("acme")) == "ocpp.events.acme")
      },
      test("ocppCommands embeds the tenantId in the topic name") {
        assertTrue(Topics.ocppCommands(TenantId("acme")) == "ocpp.commands.acme")
      },
      test("per-tenant topics are distinct for different tenants") {
        val t1 = Topics.ocppEvents(TenantId("tenant-1"))
        val t2 = Topics.ocppEvents(TenantId("tenant-2"))
        assertTrue(t1 != t2)
      },
      test("all global topic names match the canonical constants") {
        assertTrue(Topics.transactionLifecycle == "transactions.lifecycle") &&
        assertTrue(Topics.smartChargingTriggers == "smart-charging.triggers") &&
        assertTrue(Topics.notificationsOutbound == "notifications.outbound") &&
        assertTrue(Topics.billingAsyncTasks == "billing.async-tasks") &&
        assertTrue(Topics.assetConsumptions == "asset.consumptions") &&
        assertTrue(Topics.auditLog == "audit.log") &&
        assertTrue(Topics.deadLetterQueue == "dlq.failed-messages")
      }
    ),

    // ── Producer + consumer integration ────────────────────────────────────

    suite("EvKafkaProducer")(
      test("publish serialises the event as JSON and delivers it to the topic") {
        val topic = "it-publish-basic"
        val event = SampleEvent("station-boot", 0.0)
        for
          env <- ZIO.service[KafkaTestEnv]
          _   <- env.producer.publish(topic, "partition-key-1", event)
          msg <- consumeFirst(env.bootstrapServers, topic)
        yield assertTrue(msg.contains(""""name":"station-boot"""")) &&
          assertTrue(msg.contains(""""value":0.0"""))
      },
      test("publishOcppEvent routes to the per-tenant OCPP topic") {
        val tenant = TenantId("ev-tenant-42")
        val event  = SampleEvent("BootNotification", 0.0)
        for
          env <- ZIO.service[KafkaTestEnv]
          _   <- env.producer.publishOcppEvent(tenant, "CS-001", event)
          msg <- consumeFirst(env.bootstrapServers, Topics.ocppEvents(tenant))
        yield assertTrue(msg.contains("BootNotification"))
      },
      test("messages published to different topics stay on their own topic") {
        val topicA = "it-isolation-topic-a"
        val topicB = "it-isolation-topic-b"
        for
          env  <- ZIO.service[KafkaTestEnv]
          _    <- env.producer.publish(topicA, "k", SampleEvent("event-for-a", 1.0))
          _    <- env.producer.publish(topicB, "k", SampleEvent("event-for-b", 2.0))
          msgA <- consumeFirst(env.bootstrapServers, topicA)
          msgB <- consumeFirst(env.bootstrapServers, topicB)
        yield assertTrue(msgA.contains("event-for-a")) &&
          assertTrue(!msgA.contains("event-for-b")) &&
          assertTrue(msgB.contains("event-for-b")) &&
          assertTrue(!msgB.contains("event-for-a"))
      },
      test("OCPP topics for different tenants are isolated") {
        val tenantX = TenantId("tenant-x-iso")
        val tenantY = TenantId("tenant-y-iso")
        for
          env  <- ZIO.service[KafkaTestEnv]
          _    <- env.producer.publishOcppEvent(tenantX, "CS-1", SampleEvent("x-event", 0.0))
          _    <- env.producer.publishOcppEvent(tenantY, "CS-1", SampleEvent("y-event", 0.0))
          msgX <- consumeFirst(env.bootstrapServers, Topics.ocppEvents(tenantX))
          msgY <- consumeFirst(env.bootstrapServers, Topics.ocppEvents(tenantY))
        yield assertTrue(msgX.contains("x-event")) &&
          assertTrue(!msgX.contains("y-event")) &&
          assertTrue(msgY.contains("y-event")) &&
          assertTrue(!msgY.contains("x-event"))
      },
      test("publish with multiple messages preserves partition key ordering") {
        val topic = "it-ordering"
        val events = List(
          SampleEvent("first", 1.0),
          SampleEvent("second", 2.0),
          SampleEvent("third", 3.0)
        )
        for
          env <- ZIO.service[KafkaTestEnv]
          // Publish all to the same partition key so ordering is guaranteed
          _ <- ZIO.foreach(events)(e => env.producer.publish(topic, "same-key", e))
          // Consume first 3 messages
          msgs <- ZIO.scoped {
            Consumer
              .make(
                ConsumerSettings(env.bootstrapServers)
                  .withGroupId(s"it-ordering-${java.util.UUID.randomUUID()}")
                  .withProperty("auto.offset.reset", "earliest")
              )
              .flatMap { consumer =>
                val layer = ZLayer.succeed(consumer)
                Consumer
                  .plainStream(Subscription.topics(topic), Serde.string, Serde.string)
                  .take(3)
                  .map(_.record.value())
                  .runCollect
                  .provide(layer)
              }
          }
        yield assertTrue(msgs.size == 3) &&
          assertTrue(msgs(0).contains("first")) &&
          assertTrue(msgs(1).contains("second")) &&
          assertTrue(msgs(2).contains("third"))
      }
    )
  ) @@ TestAspect.sequential // one test at a time — Kafka consumer groups need clean isolation
    @@ TestAspect.timeout(120.seconds)
