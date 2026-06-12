package io.itbeans.ev.billingservice.pact

import io.circe.parser.decode
import io.itbeans.ev.billingservice.TransactionLifecycleBillingPayload
import zio.test._

// ---------------------------------------------------------------------------
// Contract: ev-billing-service (consumer) ← ev-ocpp-processor (provider)
//
// The billing service subscribes to transactions.lifecycle. When the OCPP
// processor publishes a "Stop" event the billing service creates a Stripe
// draft invoice. This spec pins the exact JSON shapes the processor emits
// (see OcppEventProcessor.publishTransactionLifecycle / -Ended) and proves
// the billing decoder accepts them — the contract test that would have
// caught the original publisher/consumer schema mismatch.
// ---------------------------------------------------------------------------

object TransactionLifecyclePactSpec extends ZIOSpecDefault:

  // Verbatim shape of OcppEventProcessor.publishTransactionLifecycleEnded
  // (OCPP 1.6 numeric transactionId, priced session).
  private val stopEventJson =
    """{
      |  "tenantId":          "tenant-abc",
      |  "chargingStationId": "CS-001",
      |  "transactionId":     12345,
      |  "action":            "Stop",
      |  "connectorId":       1,
      |  "stopReason":        "Remote",
      |  "meterValue":        22500.0,
      |  "timestamp":         1717239000000,
      |  "userId":            "user-xyz",
      |  "meterStart":        0.0,
      |  "meterStop":         22500.0,
      |  "startDate":         "2024-06-01T10:00:00Z",
      |  "endDate":           "2024-06-01T11:30:00Z",
      |  "totalDurationSecs": 5400,
      |  "consumptionWh":     22500.0,
      |  "priceUnit":         "eur",
      |  "totalCost":         6.75,
      |  "invoiceItemJson":   "[{\"type\":\"ENERGY\",\"unitPrice\":0.30,\"quantity\":22.5,\"amountCents\":675,\"description\":\"22.5 kWh @ 0.30/kWh\"}]"
      |}""".stripMargin

  // Stop event without pricing (priceUnit/totalCost/invoiceItemJson null).
  private val unpricedStopEventJson =
    """{
      |  "tenantId":          "tenant-abc",
      |  "chargingStationId": "CS-002",
      |  "transactionId":     12346,
      |  "action":            "Stop",
      |  "connectorId":       2,
      |  "stopReason":        "EVDisconnected",
      |  "meterValue":        7200.0,
      |  "timestamp":         1717245600000,
      |  "userId":            null,
      |  "meterStart":        0.0,
      |  "meterStop":         7200.0,
      |  "startDate":         "2024-06-01T12:00:00Z",
      |  "endDate":           "2024-06-01T13:00:00Z",
      |  "totalDurationSecs": 3600,
      |  "consumptionWh":     7200.0,
      |  "priceUnit":         null,
      |  "totalCost":         null,
      |  "invoiceItemJson":   null
      |}""".stripMargin

  // Verbatim shape of OcppEventProcessor.publishTransactionLifecycle (Start).
  private val startEventJson =
    """{
      |  "tenantId":          "tenant-abc",
      |  "chargingStationId": "CS-001",
      |  "transactionId":     12347,
      |  "action":            "Start",
      |  "connectorId":       1,
      |  "userId":            "user-xyz",
      |  "meterValue":        0.0,
      |  "meterStart":        0.0,
      |  "timestamp":         1717250400000
      |}""".stripMargin

  // OCPP 2.x stations may use numeric-string transaction ids; the processor
  // publishes them as JSON strings and the billing decoder must coerce.
  private val numericStringIdJson =
    stopEventJson.replace(""""transactionId":     12345""", """"transactionId":     "12345"""")

  override def spec = suite("transactions.lifecycle contract (ev-billing-service consumer)")(
    test("Stop event with pricing decodes into the billing payload") {
      val result = decode[TransactionLifecycleBillingPayload](stopEventJson)
      assertTrue(
        result.isRight,
        result.toOption.exists(_.action == "Stop"),
        result.toOption.exists(_.transactionId == 12345L),
        result.toOption.exists(_.userId.contains("user-xyz")),
        result.toOption.exists(_.totalCost.contains(6.75)),
        result.toOption.exists(_.invoiceItemJson.exists(_.contains("ENERGY")))
      )
    },
    test("Stop event without pricing decodes (nullable price fields)") {
      val result = decode[TransactionLifecycleBillingPayload](unpricedStopEventJson)
      assertTrue(
        result.isRight,
        result.toOption.exists(_.userId.isEmpty),
        result.toOption.exists(_.totalCost.isEmpty)
      )
    },
    test("Start event decodes (billing ignores the action downstream)") {
      val result = decode[TransactionLifecycleBillingPayload](startEventJson)
      assertTrue(
        result.isRight,
        result.toOption.exists(_.action == "Start")
      )
    },
    test("numeric-string transactionId (OCPP 2.x) is coerced to Long") {
      val result = decode[TransactionLifecycleBillingPayload](numericStringIdJson)
      assertTrue(
        result.isRight,
        result.toOption.exists(_.transactionId == 12345L)
      )
    }
  )
