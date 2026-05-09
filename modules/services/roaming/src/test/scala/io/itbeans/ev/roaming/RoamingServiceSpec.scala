package io.itbeans.ev.roaming

import io.circe.parser.decode
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.kafka.{EvKafkaProducer, KafkaConfig}
import io.circe.Encoder
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

object RoamingServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val tenantId     = TenantId("test-tenant")
  private val now          = Instant.parse("2024-06-15T10:00:00Z")
  private val oneHourLater = now.plusSeconds(3600)

  private val testTransaction = RoamingTransaction(
    id = 12345L,
    chargingStationId = "CS-001",
    connectorId = 1,
    evseId = None,
    tagId = Some("AABBCCDD"),
    userId = Some("user-1"),
    currency = Some("EUR"),
    startDate = now,
    endDate = oneHourLater,
    meterStart = 0.0,
    meterStop = 15000.0, // 15 kWh
    totalConsumptionWh = 15000.0,
    totalDurationSecs = 3600L,
    totalInactivitySecs = 300L, // 5 minutes parking
    totalCost = Some(3.75),
    siteId = Some("site-1"),
    ocpiSessionId = Some("SESSION-12345"),
    ocpiEvseId = Some("DE*EVX*E001*1")
  )

  private val testOcpiEndpoint = OcpiEndpoint(
    id = "ep-1",
    tenantId = "test-tenant",
    countryCode = "DE",
    partyId = "CPX",
    role = OcpiRole.EMSP,
    name = "Test EMSP",
    baseUrl = "https://ocpi.test-emsp.example.com",
    token = "their-token-123",
    localToken = "our-token-456",
    status = OcpiEndpointStatus.Registered,
    backgroundPatchJob = false,
    locationsUrl = None,
    sessionsUrl = None,
    cdrsUrl = Some("https://ocpi.test-emsp.example.com/cdrs"),
    tariffUrl = None,
    tokensUrl = None,
    commandsUrl = None,
    lastPatchJobOn = None,
    lastPatchJobResult = None,
    createdOn = now,
    lastChangedOn = now
  )

  private val testOicpEndpoint = OicpEndpoint(
    id = "oicp-1",
    tenantId = "test-tenant",
    name = "Hubject Test",
    baseUrl = "https://hubject-test.example.com",
    countryCode = "DE",
    partyId = "EVX",
    status = OicpEndpointStatus.Registered,
    operatorId = "+49*EVX",
    operatorName = "Test Operator",
    lastPatchJobOn = None,
    lastPatchJobResult = None,
    createdOn = now,
    lastChangedOn = now
  )

  // ── In-memory stubs ────────────────────────────────────────────────────────

  class MemOcpiEndpointRepository extends OcpiEndpointRepository:
    private var store: Map[String, OcpiEndpoint] = Map.empty

    def findAll(tid: TenantId): Task[List[OcpiEndpoint]] =
      ZIO.succeed(store.values.toList)

    def findById(tid: TenantId, id: String): Task[Option[OcpiEndpoint]] =
      ZIO.succeed(store.get(id))

    def findByLocalToken(tid: TenantId, token: String): Task[Option[OcpiEndpoint]] =
      ZIO.succeed(store.values.find(_.localToken == token))

    def findRegistered(tid: TenantId): Task[List[OcpiEndpoint]] =
      ZIO.succeed(store.values.filter(_.status == OcpiEndpointStatus.Registered).toList)

    def upsert(tid: TenantId, ep: OcpiEndpoint): Task[Unit] =
      ZIO.succeed { store = store.updated(ep.id, ep) }

    def delete(tid: TenantId, id: String): Task[Unit] =
      ZIO.succeed { store = store.removed(id) }
    def get(id: String): Option[OcpiEndpoint] = store.get(id)

  class MemOicpEndpointRepository extends OicpEndpointRepository:
    private var store: Map[String, OicpEndpoint] = Map.empty

    def findAll(tid: TenantId): Task[List[OicpEndpoint]] =
      ZIO.succeed(store.values.toList)

    def findById(tid: TenantId, id: String): Task[Option[OicpEndpoint]] =
      ZIO.succeed(store.get(id))

    def findRegistered(tid: TenantId): Task[List[OicpEndpoint]] =
      ZIO.succeed(store.values.filter(_.status == OicpEndpointStatus.Registered).toList)

    def upsert(tid: TenantId, ep: OicpEndpoint): Task[Unit] =
      ZIO.succeed { store = store.updated(ep.id, ep) }

    def delete(tid: TenantId, id: String): Task[Unit] =
      ZIO.succeed { store = store.removed(id) }

  class MemTransactionRepo(txs: Map[Long, RoamingTransaction] = Map.empty) extends TransactionReadRepository:

    def findById(tid: TenantId, id: Long): Task[Option[RoamingTransaction]] =
      ZIO.succeed(txs.get(id))

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("RoamingServiceSpec")(
    // ── OcpiEndpoint Circe codec ──────────────────────────────────────────────

    suite("OcpiEndpoint codec")(
      test("round-trips through JSON") {
        val json    = testOcpiEndpoint.asJson.noSpaces
        val decoded = decode[OcpiEndpoint](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.contains(testOcpiEndpoint))
      },
      test("OcpiRole codes are preserved") {
        assertTrue(OcpiRole.CPO.code == "CPO") &&
        assertTrue(OcpiRole.EMSP.code == "EMSP")
      },
      test("OcpiEndpointStatus round-trips through fromCode") {
        val statuses = OcpiEndpointStatus.values.toList
        assertTrue(statuses.forall(s => OcpiEndpointStatus.fromCode(s.code).contains(s)))
      }
    ),

    // ── OcpiCdr codec ─────────────────────────────────────────────────────────

    suite("OcpiCdr codec")(
      test("snake_case field names preserved in JSON") {
        val cdrSvc = new CdrService
        val cdr    = cdrSvc.buildOcpiCdr(testTransaction, "EUR")
        val json   = cdr.asJson
        // Verify critical snake_case field names for OCPI protocol compliance
        assertTrue(json.hcursor.downField("start_date_time").succeeded) &&
        assertTrue(json.hcursor.downField("stop_date_time").succeeded) &&
        assertTrue(json.hcursor.downField("total_energy").succeeded) &&
        assertTrue(json.hcursor.downField("total_time").succeeded) &&
        assertTrue(json.hcursor.downField("auth_method").succeeded) &&
        assertTrue(json.hcursor.downField("charging_periods").succeeded)
      },
      test("round-trips through JSON") {
        val cdrSvc  = new CdrService
        val cdr     = cdrSvc.buildOcpiCdr(testTransaction, "EUR")
        val json    = cdr.asJson.noSpaces
        val decoded = decode[OcpiCdr](json)
        assertTrue(decoded.isRight)
      }
    ),

    // ── CdrService ────────────────────────────────────────────────────────────

    suite("CdrService")(
      test("OCPI CDR: total_energy is (meterStop - meterStart) / 1000 kWh") {
        val svc = new CdrService
        val cdr = svc.buildOcpiCdr(testTransaction, "EUR")
        // 15000 Wh / 1000 = 15 kWh
        assertTrue(cdr.total_energy == 15.0)
      },
      test("OCPI CDR: total_time is duration in hours") {
        val svc = new CdrService
        val cdr = svc.buildOcpiCdr(testTransaction, "EUR")
        // 3600 seconds = 1 hour
        assertTrue(cdr.total_time == 1.0)
      },
      test("OCPI CDR: total_parking_time populated when inactivity > 0") {
        val svc = new CdrService
        val cdr = svc.buildOcpiCdr(testTransaction, "EUR")
        // 300 seconds = 0.0833 hours
        assertTrue(cdr.total_parking_time.exists(_ > 0))
      },
      test("OCPI CDR: no parking time when inactivity is 0") {
        val svc = new CdrService
        val tx  = testTransaction.copy(totalInactivitySecs = 0L)
        val cdr = svc.buildOcpiCdr(tx, "EUR")
        assertTrue(cdr.total_parking_time.isEmpty)
      },
      test("OCPI CDR: auth_id is the tagId") {
        val svc = new CdrService
        val cdr = svc.buildOcpiCdr(testTransaction, "EUR")
        assertTrue(cdr.auth_id == "AABBCCDD")
      },
      test("OCPI CDR: falls back to userId when tagId is None") {
        val svc = new CdrService
        val tx  = testTransaction.copy(tagId = None)
        val cdr = svc.buildOcpiCdr(tx, "EUR")
        assertTrue(cdr.auth_id == "user-1")
      },
      test("OCPI CDR: uses ocpiSessionId as CDR id when present") {
        val svc = new CdrService
        val cdr = svc.buildOcpiCdr(testTransaction, "EUR")
        assertTrue(cdr.id == "SESSION-12345")
      },
      test("OCPI CDR: uses tx id as CDR id when no ocpiSessionId") {
        val svc = new CdrService
        val tx  = testTransaction.copy(ocpiSessionId = None)
        val cdr = svc.buildOcpiCdr(tx, "EUR")
        assertTrue(cdr.id.contains("12345"))
      },
      test("OCPI CDR: charging period contains ENERGY and TIME dimensions") {
        val svc   = new CdrService
        val cdr   = svc.buildOcpiCdr(testTransaction, "EUR")
        val types = cdr.charging_periods.flatMap(_.dimensions).map(_.`type`)
        assertTrue(types.contains("ENERGY")) && assertTrue(types.contains("TIME"))
      },
      test("OICP CDR: ConsumedEnergy is in kWh") {
        val svc = new CdrService
        val cdr = svc.buildOicpCdr(testTransaction, "+49*EVX")
        assertTrue(cdr.ConsumedEnergy == 15.0)
      },
      test("OICP CDR: MeterValueStart/End in kWh (Wh / 1000)") {
        val svc = new CdrService
        val cdr = svc.buildOicpCdr(testTransaction, "+49*EVX")
        assertTrue(cdr.MeterValueStart == 0.0) &&
        assertTrue(cdr.MeterValueEnd == 15.0)
      },
      test("OICP CDR: RFID identification populated from tagId") {
        val svc = new CdrService
        val cdr = svc.buildOicpCdr(testTransaction, "+49*EVX")
        assertTrue(cdr.Identification.RFIDMifareFamilyIdentification.exists(_.UID == "AABBCCDD"))
      },
      test("OICP CDR: Remote identification used when no tagId") {
        val svc = new CdrService
        val tx  = testTransaction.copy(tagId = None)
        val cdr = svc.buildOicpCdr(tx, "+49*EVX")
        assertTrue(cdr.Identification.RemoteIdentification.exists(_.EvcoID == "user-1"))
      },
      test("negative energy clamped to 0") {
        val svc = new CdrService
        val tx  = testTransaction.copy(meterStart = 20000.0, meterStop = 0.0)
        val cdr = svc.buildOcpiCdr(tx, "EUR")
        assertTrue(cdr.total_energy == 0.0)
      }
    ),

    // ── OcpiEndpointRepository (in-memory) ──────────────────────────────────

    suite("OcpiEndpointRepository")(
      test("upsert and findById round-trip") {
        val repo = new MemOcpiEndpointRepository
        for
          _   <- repo.upsert(tenantId, testOcpiEndpoint)
          got <- repo.findById(tenantId, "ep-1")
        yield assertTrue(got.contains(testOcpiEndpoint))
      },
      test("findRegistered filters by status") {
        val repo    = new MemOcpiEndpointRepository
        val pending = testOcpiEndpoint.copy(id = "ep-2", status = OcpiEndpointStatus.Unregistered)
        for
          _   <- repo.upsert(tenantId, testOcpiEndpoint)
          _   <- repo.upsert(tenantId, pending)
          reg <- repo.findRegistered(tenantId)
        yield assertTrue(reg.length == 1) && assertTrue(reg.head.id == "ep-1")
      },
      test("findByLocalToken looks up by localToken field") {
        val repo = new MemOcpiEndpointRepository
        for
          _   <- repo.upsert(tenantId, testOcpiEndpoint)
          got <- repo.findByLocalToken(tenantId, "our-token-456")
        yield assertTrue(got.isDefined)
      },
      test("delete removes the endpoint") {
        val repo = new MemOcpiEndpointRepository
        for
          _   <- repo.upsert(tenantId, testOcpiEndpoint)
          _   <- repo.delete(tenantId, "ep-1")
          got <- repo.findById(tenantId, "ep-1")
        yield assertTrue(got.isEmpty)
      }
    ),

    // ── TransactionLifecyclePayload codec ─────────────────────────────────────

    suite("TransactionLifecyclePayload codec")(
      test("decodes Stop action from JSON") {
        val json =
          """{"tenantId":"t1","transactionId":123,"action":"Stop","chargingStationId":"CS-1","connectorId":1,"meterValue":15000.0,"stopReason":null}"""
        val result = decode[TransactionLifecyclePayload](json)
        assertTrue(result.isRight) &&
        assertTrue(result.toOption.exists(_.action == "Stop"))
      },
      test("decodes Start action from JSON") {
        val json =
          """{"tenantId":"t1","transactionId":123,"action":"Start","chargingStationId":"CS-1","connectorId":1,"meterValue":0.0}"""
        val result = decode[TransactionLifecyclePayload](json)
        assertTrue(result.isRight) &&
        assertTrue(result.toOption.exists(_.action == "Start"))
      }
    ),

    // ── OcpiResponse wrapper ──────────────────────────────────────────────────

    suite("OcpiResponse")(
      test("success sets status_code 1000") {
        val resp = OcpiResponse.success("data")
        assertTrue(resp.status_code == 1000) &&
        assertTrue(resp.data.contains("data"))
      },
      test("error sets correct status code and message") {
        val resp = OcpiResponse.error(2001, "Invalid params")
        assertTrue(resp.status_code == 2001) &&
        assertTrue(resp.status_message.contains("Invalid params")) &&
        assertTrue(resp.data.isEmpty)
      }
    ),

    // ── OicpEndpointStatus ────────────────────────────────────────────────────

    suite("OicpEndpointStatus")(
      test("all codes round-trip through fromCode") {
        val statuses = OicpEndpointStatus.values.toList
        assertTrue(statuses.forall(s => OicpEndpointStatus.fromCode(s.code).contains(s)))
      }
    )
  )
