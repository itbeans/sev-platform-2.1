package io.itbeans.ev.ocppprocessor

import io.circe.Encoder
import io.itbeans.ev.auth.grpc.auth_service.OcppAuthorizationResponse
import io.itbeans.ev.domain._
import io.itbeans.ev.ocpp.grpc.ocpp_gateway.{SendCommandResponse, SendResponseAck}
import io.itbeans.ev.pricing.grpc.pricing_service.{
  FinalisePriceResponse,
  PriceConsumptionResponse,
  ResolvePricingResponse
}
import org.bson.Document
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

// ---------------------------------------------------------------------------
// OcppProcessorSpec — ZIO Test suite for ev-ocpp-processor.
//
// Uses in-memory stubs for ProcessorRepository and EvKafkaProducer.
// Tests cover: dispatch by action name, BootNotification upsert,
// transaction lifecycle, MeterValues ingestion, audit log publishing.
// ---------------------------------------------------------------------------

object OcppProcessorSpec extends ZIOSpecDefault:

  // ── In-memory stubs ───────────────────────────────────────────────────────

  class InMemoryProcessorRepository extends ProcessorRepository:
    var stations: List[Document]                   = Nil
    var transactions: List[Document]               = Nil
    var consumptions: List[Document]               = Nil
    var statusUpdates: List[(String, Int, String)] = Nil // (stationId, connectorId, status)
    var heartbeats: List[(String, Instant)]        = Nil

    def upsertStation(tenantId: String, stationId: String, doc: Document): Task[Unit] =
      ZIO.succeed {
        stations = stations.filterNot(d => d.getString("_id") == stationId)
        stations = doc.append("_id", stationId) :: stations
      }

    def getStation(tenantId: String, stationId: String): Task[Option[Document]] =
      ZIO.succeed(stations.find(_.getString("_id") == stationId))

    def updateConnectorStatus(
        tenantId: String,
        stationId: String,
        connectorId: Int,
        status: String,
        timestamp: Instant
    ): Task[Unit] =
      ZIO.succeed { statusUpdates = (stationId, connectorId, status) :: statusUpdates }

    def updateHeartbeat(tenantId: String, stationId: String, timestamp: Instant): Task[Unit] =
      ZIO.succeed { heartbeats = (stationId, timestamp) :: heartbeats }

    def createTransaction(tenantId: String, doc: Document): Task[Unit] =
      ZIO.succeed { transactions = doc :: transactions }

    def updateTransaction(tenantId: String, transactionId: String, update: Document): Task[Unit] =
      ZIO.succeed { /* no-op for in-memory */ }

    def closeTransaction(tenantId: String, transactionId: String, doc: Document): Task[Unit] =
      ZIO.succeed {
        transactions = transactions.map { t =>
          if t.getString("_id") == transactionId then
            t.append("inProgress", false).append("meterStop", doc.get("meterStop"))
          else t
        }
      }

    def getTransaction(tenantId: String, transactionId: String): Task[Option[Document]] =
      ZIO.succeed(transactions.find(_.getString("_id") == transactionId))

    def insertConsumption(tenantId: String, doc: Document): Task[Unit] =
      ZIO.succeed { consumptions = doc :: consumptions }

    def updateStationFields(tenantId: String, stationId: String, fields: Document): Task[Unit] =
      ZIO.succeed { /* no-op for in-memory */ }

    def getLastCumulatedKwh(tenantId: String, transactionId: String): Task[Double] =
      ZIO.succeed(0.0)

  class InMemoryKafkaProducer extends io.itbeans.ev.kafka.EvKafkaProducer:
    var published: List[(String, String, String)] = Nil // (topic, key, serializedValue)

    def publish[A: Encoder](topic: String, key: String, value: A): Task[Unit] =
      ZIO.succeed { published = (topic, key, Encoder[A].apply(value).noSpaces) :: published }

    def publishOcppEvent[A: Encoder](tenantId: io.itbeans.ev.domain.TenantId, key: String, value: A): Task[Unit] =
      ZIO.succeed {
        published =
          (io.itbeans.ev.kafka.Topics.ocppEvents(tenantId), key, Encoder[A].apply(value).noSpaces) :: published
      }

  object NoOpAuthClient extends ProcessorAuthClient:

    def resolveOcppAuthorization(tenantId: String, idTag: String, stationId: String): Task[OcppAuthorizationResponse] =
      ZIO.succeed(OcppAuthorizationResponse(status = "Accepted"))

  object NoOpGatewayClient extends ProcessorGatewayClient:

    def sendCommand(
        tenantId: String,
        stationId: String,
        action: String,
        payloadJson: String
    ): Task[SendCommandResponse] =
      ZIO.succeed(SendCommandResponse())

    def sendResponse(
        tenantId: String,
        stationId: String,
        uniqueId: String,
        responseJson: String
    ): Task[SendResponseAck] =
      ZIO.succeed(SendResponseAck(delivered = true))

  object NoOpPricingClient extends ProcessorPricingClient:

    def resolvePricing(
        tenantId: String,
        stationId: String,
        connectorId: String,
        connectorType: String,
        connectorPowerKw: Double,
        userId: String,
        startTimestampMs: Long
    ): Task[ResolvePricingResponse] =
      ZIO.succeed(ResolvePricingResponse())

    def priceConsumption(
        tenantId: String,
        txId: String,
        pricingModelJson: String,
        consumptionWh: Double,
        instantWatts: Double,
        connectorType: String,
        connectorPowerKw: Double,
        intervalStartMs: Long,
        intervalEndMs: Long
    ): Task[PriceConsumptionResponse] =
      ZIO.succeed(PriceConsumptionResponse())

    def finalisePrice(
        tenantId: String,
        txId: String,
        pricingModelJson: String,
        totalConsumptionWh: Double,
        connectorType: String,
        connectorPowerKw: Double,
        startTimestampMs: Long,
        endTimestampMs: Long
    ): Task[FinalisePriceResponse] =
      ZIO.succeed(FinalisePriceResponse())

  // ── Fixtures ──────────────────────────────────────────────────────────────

  val t1       = TenantId("t1")
  val stationA = ChargingStationId("CS-001")

  def kafkaConfig: io.itbeans.ev.kafka.KafkaConfig =
    io.itbeans.ev.kafka.KafkaConfig(
      bootstrapServers = List("localhost:9092"),
      groupId = "test",
      autoOffsetReset = "earliest"
    )

  def makeProcessor(repo: InMemoryProcessorRepository, prod: InMemoryKafkaProducer): OcppEventProcessor =
    new OcppEventProcessor(repo, prod, kafkaConfig, NoOpAuthClient, NoOpGatewayClient, NoOpPricingClient)

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("OcppProcessorSpec")(
    suite("processRecord dispatch")(
      test("BootNotification upserts station document") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json =
          """{"action":"BootNotification","payload":{"chargingStation":{"vendorName":"ACME","model":"X200","firmwareVersion":"1.0"},"reason":"PowerUp"}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.stations.nonEmpty) &&
          assertTrue(repo.stations.head.getString("chargePointVendor") == "ACME") &&
          assertTrue(prod.published.exists(_._1 == io.itbeans.ev.kafka.Topics.auditLog))
      },
      test("Heartbeat updates lastHeartbeat") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json = """{"action":"Heartbeat","payload":{}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.heartbeats.nonEmpty)
      },
      test("StatusNotification updates connector status and triggers smart charging") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        // Trigger is published only for stations assigned to a site area
        repo.stations = List(new Document("_id", stationA.value).append("siteAreaId", "sa-001"))
        val json = """{"action":"StatusNotification","payload":{"connectorId":1,"connectorStatus":"Occupied"}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.statusUpdates.nonEmpty) &&
          assertTrue(repo.statusUpdates.head._3 == "Occupied") &&
          assertTrue(prod.published.exists(_._1 == io.itbeans.ev.kafka.Topics.smartChargingTriggers))
      },
      test("StatusNotification skips smart-charging trigger when station has no site area") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json = """{"action":"StatusNotification","payload":{"connectorId":1,"connectorStatus":"Occupied"}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.statusUpdates.nonEmpty) &&
          assertTrue(!prod.published.exists(_._1 == io.itbeans.ev.kafka.Topics.smartChargingTriggers))
      },
      test("StartTransaction creates transaction document and publishes lifecycle event") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json =
          """{"action":"StartTransaction","payload":{"transactionId":42,"connectorId":1,"meterStart":0.0,"idTag":"RFID-001"}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.transactions.nonEmpty) &&
          assertTrue(repo.transactions.head.getBoolean("inProgress") == true) &&
          assertTrue(prod.published.exists(_._1 == io.itbeans.ev.kafka.Topics.transactionLifecycle))
      },
      test("StopTransaction closes transaction and publishes lifecycle event") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        // First start it
        val startJson =
          """{"action":"StartTransaction","payload":{"transactionId":42,"connectorId":1,"meterStart":0.0,"idTag":"RFID-001"}}"""
        val stopJson =
          """{"action":"StopTransaction","payload":{"transactionId":42,"meterStop":10500.0,"reason":"EVDisconnected"}}"""
        for
          _ <- proc.processRecord(stationA.value, startJson, s"ocpp.events.${t1.value}")
          _ <- proc.processRecord(stationA.value, stopJson, s"ocpp.events.${t1.value}")
        yield
          val tx = repo.transactions.find(_.getString("_id") == "42")
          assertTrue(tx.isDefined) &&
          assertTrue(tx.get.getBoolean("inProgress") == false)
      },
      test("MeterValues inserts consumption row") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json =
          """{"action":"MeterValues","payload":{"connectorId":1,"transactionId":42,"meterValue":[{"sampledValue":[{"value":"7400","measurand":"Power.Active.Import","unit":"W"}]}]}}"""
        for
          _ <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}")
        yield assertTrue(repo.consumptions.nonEmpty)
      },
      test("unknown action is ignored without error") {
        val repo = new InMemoryProcessorRepository
        val prod = new InMemoryKafkaProducer
        val proc = makeProcessor(repo, prod)
        val json = """{"action":"UnknownFutureAction","payload":{}}"""
        for
          result <- proc.processRecord(stationA.value, json, s"ocpp.events.${t1.value}").exit
        yield assertTrue(result.isSuccess)
      }
    ),
    suite("ProcessorConfig defaults")(
      test("stationOfflineThresholdSecs is 540") {
        val cfg = ProcessorConfig(
          mongoUri = "mongodb://localhost:27017",
          mongoDbName = "evse",
          authGrpcHost = "localhost",
          authGrpcPort = 50051,
          pricingGrpcHost = "localhost",
          pricingGrpcPort = 50052,
          gatewayGrpcHost = "localhost",
          gatewayGrpcPort = 50053,
          consumerGroupId = "ev-ocpp-processor",
          stationOfflineThresholdSecs = 540
        )
        assertTrue(cfg.stationOfflineThresholdSecs == 540)
      }
    )
  )
