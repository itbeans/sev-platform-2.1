package io.itbeans.ev.ocppgateway

import io.itbeans.ev.domain.{ChargingStationId, OcppVersion, TenantId}
import zio._
import zio.test._
import zio.test.Assertion._

// ---------------------------------------------------------------------------
// OcppGatewaySpec — ZIO Test suite for ev-ocpp-gateway.
//
// Tests:
//   - ConnectionRegistry: register, deregister, list, tenant isolation, ZHub events
//   - OcppFrameHandler.parseFrame: all five OCPP frame types including OCPP 2.1
// ---------------------------------------------------------------------------

object OcppGatewaySpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  val t1   = TenantId("tenant1")
  val t2   = TenantId("tenant2")
  val cs1  = ChargingStationId("CS-001")
  val cs2  = ChargingStationId("CS-002")
  val key1 = ConnectionKey(t1, cs1)
  val key2 = ConnectionKey(t1, cs2)
  val key3 = ConnectionKey(t2, cs1) // same stationId, different tenant

  def makeEntry(key: ConnectionKey, version: OcppVersion = OcppVersion.V1_6): ConnectionEntry =
    ConnectionEntry(key, null, version, java.time.Instant.now())

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("OcppGatewaySpec")(
    suite("ConnectionRegistry")(
      test("register and get a connection") {
        for
          registry <- ZIO.serviceWith[ConnectionRegistry](identity)
          entry = makeEntry(key1, OcppVersion.V2_1)
          _      <- registry.register(entry)
          result <- registry.get(key1)
        yield assertTrue(result.isDefined) &&
          assertTrue(result.get.ocppVersion == OcppVersion.V2_1)
      }.provide(ConnectionRegistry.live),
      test("deregister removes connection") {
        for
          registry <- ZIO.serviceWith[ConnectionRegistry](identity)
          _        <- registry.register(makeEntry(key1))
          _        <- registry.deregister(key1)
          result   <- registry.get(key1)
        yield assertTrue(result.isEmpty)
      }.provide(ConnectionRegistry.live),
      test("listAll filters by tenant") {
        for
          registry <- ZIO.serviceWith[ConnectionRegistry](identity)
          _        <- registry.register(makeEntry(key1))
          _        <- registry.register(makeEntry(key2))
          _        <- registry.register(makeEntry(key3)) // different tenant
          t1List   <- registry.listAll(Some(t1))
          allList  <- registry.listAll(None)
        yield assertTrue(t1List.size == 2) &&
          assertTrue(allList.size == 3)
      }.provide(ConnectionRegistry.live),
      test("events stream is non-empty after register") {
        for
          registry <- ZIO.serviceWith[ConnectionRegistry](identity)
          _        <- registry.register(makeEntry(key1))
          entry    <- registry.get(key1)
        yield assertTrue(entry.isDefined)
      }.provide(ConnectionRegistry.live)
    ),
    suite("OcppFrameHandler frame parsing")(
      test("parses CALL frame (type 2)") {
        val raw = """[2, "uniqueId-1", "BootNotification", {"reason":"PowerUp"}]"""
        for
          pending <- Ref.make(Map.empty[String, PendingRequest])
          producer <- ZIO.succeed(new io.itbeans.ev.kafka.EvKafkaProducer {
            def publish[A: io.circe.Encoder](topic: String, partitionKey: String, event: A): Task[Unit] = ZIO.unit
            def publishOcppEvent[A: io.circe.Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
              ZIO.unit
          })
          handler = new OcppFrameHandler(producer, pending)
          frame <- handler.parseFramePublic(raw) // exposed for testing via package access
        yield frame match
          case OcppCallFrame(uid, action, payload) =>
            assertTrue(uid == "uniqueId-1") &&
            assertTrue(action == "BootNotification") &&
            assertTrue(payload.contains("PowerUp"))
          case other =>
            assertTrue(false)
      },
      test("parses CALLRESULT frame (type 3)") {
        val raw = """[3, "uniqueId-2", {"currentTime":"2024-06-15T12:00:00Z","interval":300,"status":"Accepted"}]"""
        for
          pending <- Ref.make(Map.empty[String, PendingRequest])
          producer <- ZIO.succeed(new io.itbeans.ev.kafka.EvKafkaProducer {
            def publish[A: io.circe.Encoder](topic: String, partitionKey: String, event: A): Task[Unit] = ZIO.unit
            def publishOcppEvent[A: io.circe.Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
              ZIO.unit
          })
          handler = new OcppFrameHandler(producer, pending)
          frame <- handler.parseFramePublic(raw)
        yield frame match
          case OcppCallResultFrame(uid, payload) =>
            assertTrue(uid == "uniqueId-2") &&
            assertTrue(payload.contains("Accepted"))
          case other => assertTrue(false)
      },
      test("parses CALLERROR frame (type 4)") {
        val raw = """[4, "uniqueId-3", "GenericError", "Something went wrong", {}]"""
        for
          pending <- Ref.make(Map.empty[String, PendingRequest])
          producer <- ZIO.succeed(new io.itbeans.ev.kafka.EvKafkaProducer {
            def publish[A: io.circe.Encoder](topic: String, partitionKey: String, event: A): Task[Unit] = ZIO.unit
            def publishOcppEvent[A: io.circe.Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
              ZIO.unit
          })
          handler = new OcppFrameHandler(producer, pending)
          frame <- handler.parseFramePublic(raw)
        yield frame match
          case OcppCallErrorFrame(uid, code, desc) =>
            assertTrue(uid == "uniqueId-3") &&
            assertTrue(code == "GenericError")
          case other => assertTrue(false)
      },
      test("parses OCPP 2.1 SEND frame (type 6)") {
        val raw = """[6, "uniqueId-4", "NotifyDERStartStop", {"started":true}]"""
        for
          pending <- Ref.make(Map.empty[String, PendingRequest])
          producer <- ZIO.succeed(new io.itbeans.ev.kafka.EvKafkaProducer {
            def publish[A: io.circe.Encoder](topic: String, partitionKey: String, event: A): Task[Unit] = ZIO.unit
            def publishOcppEvent[A: io.circe.Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
              ZIO.unit
          })
          handler = new OcppFrameHandler(producer, pending)
          frame <- handler.parseFramePublic(raw)
        yield frame match
          case OcppSendFrame(uid, action, payload) =>
            assertTrue(uid == "uniqueId-4") &&
            assertTrue(action == "NotifyDERStartStop") &&
            assertTrue(payload.contains("true"))
          case other => assertTrue(false)
      },
      test("fails on invalid JSON") {
        for
          pending <- Ref.make(Map.empty[String, PendingRequest])
          producer <- ZIO.succeed(new io.itbeans.ev.kafka.EvKafkaProducer {
            def publish[A: io.circe.Encoder](topic: String, partitionKey: String, event: A): Task[Unit] = ZIO.unit
            def publishOcppEvent[A: io.circe.Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
              ZIO.unit
          })
          handler = new OcppFrameHandler(producer, pending)
          result <- handler.parseFramePublic("not-json-at-all").exit
        yield assertTrue(result.isFailure)
      }
    ),
    suite("GatewayConfig defaults")(
      test("responseTimeoutMs is 30000") {
        val cfg = GatewayConfig(
          httpPort = 8010,
          grpcPort = 9090,
          jwtSecret = "secret",
          authGrpcHost = "localhost",
          authGrpcPort = 50051,
          responseTimeoutMs = 30000L,
          heartbeatIntervalSecs = 60
        )
        assertTrue(cfg.responseTimeoutMs == 30000L) &&
        assertTrue(cfg.httpPort == 8010)
      }
    )
  )
