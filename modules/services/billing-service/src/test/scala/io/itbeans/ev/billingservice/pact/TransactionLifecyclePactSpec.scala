package io.itbeans.ev.billingservice.pact

import io.circe.syntax._
import io.circe.generic.auto._
import io.github.jbwheatley.pact4s.circe.CirceJsonEncoding
import io.github.jbwheatley.pact4s.ziotest.MessagePactForgerSuite
import io.github.jbwheatley.pact4s.{MessagePact, MessagePactForger}
import io.github.jbwheatley.pact4s.dsl.MessagePactDsl
import zio._
import zio.test._

// ---------------------------------------------------------------------------
// Message Pact: ev-billing-service (consumer) ← ev-ocpp-processor (provider)
//
// The billing service subscribes to transactions.lifecycle.  When the OCPP
// processor publishes a "Stop" event the billing service creates a Stripe
// draft invoice.  This spec pins the Kafka message schema so that the
// processor's MessagePactVerifySpec can validate every published payload
// against these expectations before production cutover.
//
// Generated pact file: target/pacts/ev-billing-service-ev-ocpp-processor.json
// ---------------------------------------------------------------------------

object TransactionLifecyclePactSpec extends MessagePactForgerSuite with CirceJsonEncoding:

  // ── Interactions ──────────────────────────────────────────────────────────

  override val pacts: List[MessagePact] = List(
    // ── Interaction 1: tx stopped with ENERGY dimension ─────────────────────
    MessagePactDsl
      .consumer("ev-billing-service")
      .hasPactWith("ev-ocpp-processor")
      .expectsToReceive("a transaction Stop event with energy billing")
      .withMetadata(Map("contentType" -> "application/json"))
      .withContent(
        """{
          |  "action":            "Stop",
          |  "tenantId":          "tenant-abc",
          |  "transactionId":     12345,
          |  "chargingStationId": "CS-001",
          |  "userId":            "user-xyz",
          |  "pricingId":         "pricing-123",
          |  "startDate":         "2024-06-01T10:00:00Z",
          |  "endDate":           "2024-06-01T11:30:00Z",
          |  "consumptionKwh":    22.5,
          |  "durationSecs":      5400,
          |  "dimensions": [
          |    {
          |      "type":       "ENERGY",
          |      "unitPrice":  0.30,
          |      "quantity":   22.5,
          |      "amountCents": 675,
          |      "description": "22.5 kWh @ £0.30/kWh"
          |    }
          |  ]
          |}""".stripMargin
      )
      .toPact,

    // ── Interaction 2: tx stopped — time + energy dimensions ────────────────
    MessagePactDsl
      .consumer("ev-billing-service")
      .hasPactWith("ev-ocpp-processor")
      .expectsToReceive("a transaction Stop event with time and energy billing")
      .withMetadata(Map("contentType" -> "application/json"))
      .withContent(
        """{
          |  "action":            "Stop",
          |  "tenantId":          "tenant-abc",
          |  "transactionId":     12346,
          |  "chargingStationId": "CS-002",
          |  "userId":            "user-xyz",
          |  "pricingId":         "pricing-456",
          |  "startDate":         "2024-06-01T12:00:00Z",
          |  "endDate":           "2024-06-01T13:00:00Z",
          |  "consumptionKwh":    7.2,
          |  "durationSecs":      3600,
          |  "dimensions": [
          |    {
          |      "type":        "CHARGING_TIME",
          |      "unitPrice":   5.00,
          |      "quantity":    1.0,
          |      "amountCents": 500,
          |      "description": "1 hour session fee"
          |    },
          |    {
          |      "type":        "ENERGY",
          |      "unitPrice":   0.25,
          |      "quantity":    7.2,
          |      "amountCents": 180,
          |      "description": "7.2 kWh @ £0.25/kWh"
          |    }
          |  ]
          |}""".stripMargin
      )
      .toPact,

    // ── Interaction 3: Start event — billing service must ignore it ──────────
    // Billing only acts on Stop.  This interaction documents that the Start
    // message shape is valid so the billing service won't crash on receipt.
    MessagePactDsl
      .consumer("ev-billing-service")
      .hasPactWith("ev-ocpp-processor")
      .expectsToReceive("a transaction Start event (billing ignores this)")
      .withMetadata(Map("contentType" -> "application/json"))
      .withContent(
        """{
          |  "action":            "Start",
          |  "tenantId":          "tenant-abc",
          |  "transactionId":     12347,
          |  "chargingStationId": "CS-001",
          |  "userId":            "user-xyz",
          |  "pricingId":         "pricing-123",
          |  "startDate":         "2024-06-01T14:00:00Z",
          |  "endDate":           null,
          |  "consumptionKwh":    0.0,
          |  "durationSecs":      0,
          |  "dimensions":        []
          |}""".stripMargin
      )
      .toPact
  )

  // ── ZIO Test suite — deserialise each message and verify business rules ───

  override val tests: Spec[MessagePactForger, Throwable] =
    suite("transactions.lifecycle message contract (ev-billing-service consumer)")(
      test("Stop event with ENERGY dimension is deserializable") {
        for
          msg <- ZIO.serviceWith[MessagePactForger](
            _.getMessage("a transaction Stop event with energy billing")
          )
          parsed <- ZIO.fromEither(io.circe.parser.decode[BillingPayloadPreview](msg.body))
        yield assertTrue(
          parsed.action == "Stop",
          parsed.consumptionKwh > 0,
          parsed.dimensions.nonEmpty,
          parsed.userId.nonEmpty
        )
      },
      test("Stop event with time + energy dimensions is deserializable") {
        for
          msg <- ZIO.serviceWith[MessagePactForger](
            _.getMessage("a transaction Stop event with time and energy billing")
          )
          parsed <- ZIO.fromEither(io.circe.parser.decode[BillingPayloadPreview](msg.body))
        yield assertTrue(
          parsed.action == "Stop",
          parsed.dimensions.size == 2
        )
      },
      test("Start event is deserializable (billing ignores action)") {
        for
          msg <- ZIO.serviceWith[MessagePactForger](
            _.getMessage("a transaction Start event (billing ignores this)")
          )
          parsed <- ZIO.fromEither(io.circe.parser.decode[BillingPayloadPreview](msg.body))
        yield assertTrue(parsed.action == "Start")
      }
    )

// Minimal projection of the Kafka payload — used only to verify deserialization
private case class DimensionPreview(`type`: String, amountCents: Int)

private case class BillingPayloadPreview(
    action: String,
    transactionId: Long,
    userId: String,
    consumptionKwh: Double,
    dimensions: List[DimensionPreview]
)
