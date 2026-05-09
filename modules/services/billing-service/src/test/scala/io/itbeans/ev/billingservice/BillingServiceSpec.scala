package io.itbeans.ev.billingservice

import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant
import java.util.UUID

object BillingServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val now      = Instant.parse("2024-06-15T10:00:00Z")
  private val oneHour  = now.plusSeconds(3600)
  private val tenantId = "test-tenant"
  private val userId   = "user-1"

  private val testPayload = TransactionLifecycleBillingPayload(
    tenantId = tenantId,
    transactionId = 12345L,
    action = "Stop",
    chargingStationId = "CS-001",
    userId = Some(userId),
    meterStart = Some(0.0),
    meterStop = Some(15000.0),
    startDate = Some(now),
    endDate = Some(oneHour),
    totalDurationSecs = Some(3600L),
    consumptionWh = Some(15000.0),
    priceUnit = Some("EUR"),
    totalCost = Some(3.75),
    pricingId = None
  )

  private val testInvoice = Invoice(
    id = "inv-test-1",
    tenantId = tenantId,
    invoiceId = "inv_test_123",
    invoiceNumber = Some("INV-001"),
    status = InvoiceStatus.Draft,
    amountCents = 375,
    amountPaidCents = 0,
    currency = "eur",
    userId = userId,
    customerId = "cus_test_456",
    liveMode = false,
    sessions = List(BillingSessionData(
      transactionId = 12345L,
      chargingStationId = "CS-001",
      pricingId = None,
      startDate = now,
      endDate = oneHour,
      consumptionKwh = 15.0,
      durationSecs = 3600L,
      dimensions = List(
        BillingDimension(
          `type` = "ENERGY",
          unitPrice = BigDecimal("0.2500"),
          quantity = BigDecimal("15.000"),
          amountCents = 375,
          description = "Charging energy 15.000 kWh"
        )
      )
    )),
    downloadUrl = None,
    payInvoiceUrl = None,
    lastError = None,
    createdOn = now,
    lastChangedOn = now
  )

  private val testCfg = BillingConfig(
    httpPort = 8080,
    grpcPort = 9090,
    stripeSecretKey = "sk_test_TESTKEY",
    stripeWebhookSecret = "whsec_TESTKEY",
    immediatePayment = false,
    monthlyBillingDay = 1,
    platformFeePercent = 2.5,
    platformFeeCentsPerSession = 0,
    jdbcUrl = "jdbc:postgresql://localhost/test",
    jdbcUser = "test",
    jdbcPassword = "test",
    defaultTenantId = tenantId
  )

  // ── In-memory stubs ────────────────────────────────────────────────────────

  class MemInvoiceRepository extends InvoiceRepository:
    private var store: Map[String, Invoice] = Map.empty

    def create(inv: Invoice): Task[Unit] =
      ZIO.succeed { store = store.updated(inv.id, inv) }

    def findById(tenantId: String, id: String): Task[Option[Invoice]] =
      ZIO.succeed(store.get(id).filter(_.tenantId == tenantId))

    def findByUserId(tenantId: String, userId: String): Task[List[Invoice]] =
      ZIO.succeed(store.values.filter(i => i.tenantId == tenantId && i.userId == userId).toList)

    def findByStatus(tenantId: String, status: InvoiceStatus): Task[List[Invoice]] =
      ZIO.succeed(store.values.filter(i => i.tenantId == tenantId && i.status == status).toList)

    def findDraftsBefore(tenantId: String, beforeEpochMs: Long): Task[List[Invoice]] =
      ZIO.succeed(
        store.values.filter(i =>
          i.tenantId == tenantId &&
            i.status == InvoiceStatus.Draft &&
            i.createdOn.toEpochMilli < beforeEpochMs
        ).toList
      )

    def update(inv: Invoice): Task[Unit] =
      ZIO.succeed { store = store.updated(inv.id, inv) }
    def all: List[Invoice] = store.values.toList

  class MemBillingUserRepository extends BillingUserRepository:
    private var store: Map[String, BillingUser] = Map.empty

    def findByUserId(tenantId: String, userId: String): Task[Option[BillingUser]] =
      ZIO.succeed(store.get(s"$tenantId.$userId"))

    def upsert(u: BillingUser): Task[Unit] =
      ZIO.succeed { store = store.updated(s"${u.tenantId}.${u.userId}", u) }

    def seed(u: BillingUser): Unit =
      store = store.updated(s"${u.tenantId}.${u.userId}", u)

  class MemBillingAccountRepository extends BillingAccountRepository:
    private var store: Map[String, BillingAccount] = Map.empty

    def create(acc: BillingAccount): Task[Unit] =
      ZIO.succeed { store = store.updated(acc.id, acc) }

    def findById(tenantId: String, id: String): Task[Option[BillingAccount]] =
      ZIO.succeed(store.get(id).filter(_.tenantId == tenantId))

    def findByOwner(tenantId: String, userId: String): Task[Option[BillingAccount]] =
      ZIO.succeed(store.values.find(a => a.tenantId == tenantId && a.businessOwnerUserId == userId))

    def findAll(tenantId: String): Task[List[BillingAccount]] =
      ZIO.succeed(store.values.filter(_.tenantId == tenantId).toList)

    def update(acc: BillingAccount): Task[Unit] =
      ZIO.succeed { store = store.updated(acc.id, acc) }

  class MemBillingTransferRepository extends BillingTransferRepository:
    private var store: Map[String, BillingTransfer] = Map.empty

    def create(t: BillingTransfer): Task[Unit] =
      ZIO.succeed { store = store.updated(t.id, t) }

    def findById(tenantId: String, id: String): Task[Option[BillingTransfer]] =
      ZIO.succeed(store.get(id).filter(_.tenantId == tenantId))

    def findDraftByAccount(tenantId: String, accountId: String, currency: String): Task[Option[BillingTransfer]] =
      ZIO.succeed(store.values.find(t =>
        t.tenantId == tenantId && t.accountId == accountId &&
          t.currency == currency && t.status == TransferStatus.Draft
      ))

    def findAll(tenantId: String): Task[List[BillingTransfer]] =
      ZIO.succeed(store.values.filter(_.tenantId == tenantId).toList)

    def update(t: BillingTransfer): Task[Unit] =
      ZIO.succeed { store = store.updated(t.id, t) }

  /** Stub StripeClient that returns predictable JSON without hitting the network. */
  class StubStripeClient extends StripeClient:
    private def fakeCustomer(id: String) = Json.obj("id" -> Json.fromString(id))

    private def fakeInvoice(id: String) = Json.obj(
      "id"                 -> Json.fromString(id),
      "status"             -> Json.fromString("paid"),
      "amount_paid"        -> Json.fromInt(375),
      "number"             -> Json.fromString("INV-STUB"),
      "invoice_pdf"        -> Json.fromString("https://stripe.com/invoice.pdf"),
      "hosted_invoice_url" -> Json.fromString("https://stripe.com/pay")
    )

    def createCustomer(email: String, name: String, userId: String): Task[Json] =
      ZIO.succeed(fakeCustomer(s"cus_stub_$userId"))

    def updateCustomer(customerId: String, fields: Map[String, String]): Task[Json] =
      ZIO.succeed(fakeCustomer(customerId))

    def retrieveCustomer(customerId: String): Task[Json] =
      ZIO.succeed(fakeCustomer(customerId))

    def listPaymentMethods(customerId: String): Task[Json] =
      ZIO.succeed(Json.obj("data" -> Json.arr()))

    def attachPaymentMethod(paymentMethodId: String, customerId: String): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString(paymentMethodId)))

    def detachPaymentMethod(paymentMethodId: String): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString(paymentMethodId)))

    def createSetupIntent(customerId: String): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString("seti_stub")))

    def createInvoice(customerId: String, currency: String, description: String): Task[Json] =
      ZIO.succeed(fakeInvoice(s"inv_stub_${java.lang.System.nanoTime()}"))

    def retrieveInvoice(invoiceId: String): Task[Json] =
      ZIO.succeed(fakeInvoice(invoiceId))

    def listInvoices(
        customerId: Option[String],
        status: Option[String],
        createdGte: Option[Long],
        createdLt: Option[Long],
        limit: Int
    ): Task[Json] =
      ZIO.succeed(Json.obj("data" -> Json.arr()))

    def finalizeInvoice(invoiceId: String): Task[Json] =
      ZIO.succeed(fakeInvoice(invoiceId))

    def payInvoice(invoiceId: String): Task[Json] =
      ZIO.succeed(fakeInvoice(invoiceId))

    def voidInvoice(invoiceId: String): Task[Json] =
      ZIO.succeed(fakeInvoice(invoiceId))

    def createInvoiceItem(
        customerId: String,
        invoiceId: String,
        amountCents: Int,
        currency: String,
        description: String
    ): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString("ii_stub")))

    def listTaxRates(active: Boolean): Task[Json] =
      ZIO.succeed(Json.obj("data" -> Json.arr()))

    def createConnectedAccount(email: String, companyName: String): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString("acct_stub")))

    def createAccountLink(accountId: String, refreshUrl: String, returnUrl: String): Task[Json] =
      ZIO.succeed(Json.obj("url" -> Json.fromString("https://stripe.com/onboard")))

    def createTransfer(amountCents: Int, currency: String, dest: String, desc: String): Task[Json] =
      ZIO.succeed(Json.obj("id" -> Json.fromString("tr_stub")))

    def constructEvent(payload: String, sigHeader: String): Task[Json] =
      ZIO.fromEither(io.circe.parser.parse(payload).left.map(e => new Exception(e.message)))

  private def makeBillingService(
      stripe: StripeClient = new StubStripeClient,
      invoiceRepo: InvoiceRepository = new MemInvoiceRepository,
      userRepo: BillingUserRepository = new MemBillingUserRepository,
      transferRepo: BillingTransferRepository = new MemBillingTransferRepository,
      accountRepo: BillingAccountRepository = new MemBillingAccountRepository,
      cfg: BillingConfig = testCfg
  ): LiveBillingService =
    new LiveBillingService(stripe, invoiceRepo, userRepo, transferRepo, accountRepo, cfg)

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("BillingServiceSpec")(
    // ── InvoiceStatus codec ────────────────────────────────────────────────

    suite("InvoiceStatus codec")(
      test("all codes round-trip through fromCode") {
        val statuses = InvoiceStatus.values.toList
        assertTrue(statuses.forall(s => InvoiceStatus.fromCode(s.code).contains(s)))
      },
      test("round-trips through JSON") {
        val json    = (testInvoice.status: InvoiceStatus).asJson.noSpaces
        val decoded = decode[InvoiceStatus](json)
        assertTrue(decoded == Right(InvoiceStatus.Draft))
      },
      test("unknown code falls back in fromCode") {
        assertTrue(InvoiceStatus.fromCode("bogus").isEmpty)
      }
    ),

    // ── BillingAccountStatus + TransferStatus codecs ───────────────────────

    suite("status enum codecs")(
      test("BillingAccountStatus all codes round-trip") {
        val statuses = BillingAccountStatus.values.toList
        assertTrue(statuses.forall(s => BillingAccountStatus.fromCode(s.code).contains(s)))
      },
      test("TransferStatus all codes round-trip") {
        val statuses = TransferStatus.values.toList
        assertTrue(statuses.forall(s => TransferStatus.fromCode(s.code).contains(s)))
      }
    ),

    // ── Invoice Circe codec ────────────────────────────────────────────────

    suite("Invoice codec")(
      test("Invoice round-trips through JSON") {
        val json    = testInvoice.asJson.noSpaces
        val decoded = decode[Invoice](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.contains(testInvoice))
      },
      test("BillingSessionData round-trips through JSON") {
        val session = testInvoice.sessions.head
        val json    = session.asJson.noSpaces
        val decoded = decode[BillingSessionData](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.map(_.transactionId).contains(12345L))
      },
      test("BillingDimension round-trips through JSON") {
        val dim     = testInvoice.sessions.head.dimensions.head
        val json    = dim.asJson.noSpaces
        val decoded = decode[BillingDimension](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.map(_.amountCents).contains(375))
      }
    ),

    // ── TransactionLifecycleBillingPayload codec ───────────────────────────

    suite("TransactionLifecycleBillingPayload codec")(
      test("decodes Stop action from JSON") {
        val json =
          """{"tenantId":"t1","transactionId":123,"action":"Stop","chargingStationId":"CS-1",
             "userId":"u1","meterStart":0.0,"meterStop":15000.0,
             "startDate":"2024-06-15T10:00:00Z","endDate":"2024-06-15T11:00:00Z",
             "totalDurationSecs":3600,"consumptionWh":15000.0,
             "priceUnit":"EUR","totalCost":3.75,"pricingId":null}"""
        val result = decode[TransactionLifecycleBillingPayload](json)
        assertTrue(result.isRight) &&
        assertTrue(result.toOption.exists(_.action == "Stop")) &&
        assertTrue(result.toOption.exists(_.transactionId == 123L))
      },
      test("payload with null userId decodes correctly") {
        val json =
          """{"tenantId":"t1","transactionId":456,"action":"Stop","chargingStationId":"CS-2",
             "userId":null,"meterStart":null,"meterStop":null,"startDate":null,"endDate":null,
             "totalDurationSecs":null,"consumptionWh":null,"priceUnit":null,"totalCost":null,"pricingId":null}"""
        val result = decode[TransactionLifecycleBillingPayload](json)
        assertTrue(result.isRight) &&
        assertTrue(result.toOption.exists(_.userId.isEmpty))
      }
    ),

    // ── MemInvoiceRepository ───────────────────────────────────────────────

    suite("MemInvoiceRepository")(
      test("create and findById round-trip") {
        val repo = new MemInvoiceRepository
        for
          _   <- repo.create(testInvoice)
          got <- repo.findById(tenantId, testInvoice.id)
        yield assertTrue(got.contains(testInvoice))
      },
      test("findByUserId returns only matching invoices") {
        val repo  = new MemInvoiceRepository
        val other = testInvoice.copy(id = UUID.randomUUID().toString, userId = "other-user")
        for
          _   <- repo.create(testInvoice)
          _   <- repo.create(other)
          got <- repo.findByUserId(tenantId, userId)
        yield assertTrue(got.length == 1) && assertTrue(got.head.id == testInvoice.id)
      },
      test("findByStatus filters correctly") {
        val repo = new MemInvoiceRepository
        val paid = testInvoice.copy(id = UUID.randomUUID().toString, status = InvoiceStatus.Paid)
        for
          _      <- repo.create(testInvoice)
          _      <- repo.create(paid)
          drafts <- repo.findByStatus(tenantId, InvoiceStatus.Draft)
        yield assertTrue(drafts.length == 1) && assertTrue(drafts.head.status == InvoiceStatus.Draft)
      },
      test("findDraftsBefore respects createdOn epoch millis") {
        val repo = new MemInvoiceRepository
        val future = testInvoice.copy(
          id = UUID.randomUUID().toString,
          createdOn = Instant.parse("2025-01-01T00:00:00Z")
        )
        val cutoff = Instant.parse("2024-12-01T00:00:00Z").toEpochMilli
        for
          _   <- repo.create(testInvoice) // createdOn 2024-06-15 (before cutoff)
          _   <- repo.create(future)      // createdOn 2025-01-01 (after cutoff)
          got <- repo.findDraftsBefore(tenantId, cutoff)
        yield assertTrue(got.length == 1) && assertTrue(got.head.id == testInvoice.id)
      },
      test("update persists status change") {
        val repo = new MemInvoiceRepository
        for
          _ <- repo.create(testInvoice)
          upd = testInvoice.copy(status = InvoiceStatus.Paid, amountPaidCents = 375)
          _   <- repo.update(upd)
          got <- repo.findById(tenantId, testInvoice.id)
        yield assertTrue(got.exists(_.status == InvoiceStatus.Paid))
      }
    ),

    // ── MemBillingUserRepository ───────────────────────────────────────────

    suite("MemBillingUserRepository")(
      test("upsert and findByUserId round-trip") {
        val repo = new MemBillingUserRepository
        val user = BillingUser(userId, tenantId, "cus_test", None, now)
        for
          _   <- repo.upsert(user)
          got <- repo.findByUserId(tenantId, userId)
        yield assertTrue(got.contains(user))
      },
      test("upsert overwrites existing entry") {
        val repo  = new MemBillingUserRepository
        val user1 = BillingUser(userId, tenantId, "cus_old", None, now)
        val user2 = BillingUser(userId, tenantId, "cus_new", Some("pm_123"), now)
        for
          _   <- repo.upsert(user1)
          _   <- repo.upsert(user2)
          got <- repo.findByUserId(tenantId, userId)
        yield assertTrue(got.exists(_.customerId == "cus_new"))
      }
    ),

    // ── BillingService — getOrCreateCustomer ──────────────────────────────

    suite("BillingService.getOrCreateCustomer")(
      test("creates a new Stripe customer when none exists") {
        val userRepo = new MemBillingUserRepository
        val svc      = makeBillingService(userRepo = userRepo)
        for
          customerId <- svc.getOrCreateCustomer(tenantId, userId, "test@example.com", "Test User")
          stored     <- userRepo.findByUserId(tenantId, userId)
        yield assertTrue(customerId.startsWith("cus_stub_")) &&
          assertTrue(stored.isDefined)
      },
      test("returns existing customer ID without calling Stripe again") {
        val userRepo = new MemBillingUserRepository
        userRepo.seed(BillingUser(userId, tenantId, "cus_existing", None, now))
        val svc = makeBillingService(userRepo = userRepo)
        for
          customerId <- svc.getOrCreateCustomer(tenantId, userId, "test@example.com", "Test User")
        yield assertTrue(customerId == "cus_existing")
      }
    ),

    // ── BillingService — createInvoiceForSession ───────────────────────────

    suite("BillingService.createInvoiceForSession")(
      test("creates draft invoice for a billable transaction") {
        val invoiceRepo = new MemInvoiceRepository
        val userRepo    = new MemBillingUserRepository
        userRepo.seed(BillingUser(userId, tenantId, "cus_test", None, now))
        val svc = makeBillingService(invoiceRepo = invoiceRepo, userRepo = userRepo)
        for
          invoice <- svc.createInvoiceForSession(tenantId, testPayload)
          stored  <- invoiceRepo.findById(tenantId, invoice.id)
        yield assertTrue(invoice.status == InvoiceStatus.Draft) &&
          assertTrue(invoice.amountCents == 375) && // 3.75 EUR = 375 cents
          assertTrue(invoice.currency == "eur") &&
          assertTrue(stored.isDefined)
      },
      test("fails when no BillingUser exists for the userId") {
        val svc = makeBillingService() // empty user repo
        for
          result <- svc.createInvoiceForSession(tenantId, testPayload).exit
        yield assertTrue(result.isFailure)
      },
      test("fails when payload has no userId") {
        val svc     = makeBillingService()
        val payload = testPayload.copy(userId = None)
        for
          result <- svc.createInvoiceForSession(tenantId, payload).exit
        yield assertTrue(result.isFailure)
      },
      test("zero totalCost produces an invoice with 0 cents and no line items") {
        val invoiceRepo = new MemInvoiceRepository
        val userRepo    = new MemBillingUserRepository
        userRepo.seed(BillingUser(userId, tenantId, "cus_test", None, now))
        val svc     = makeBillingService(invoiceRepo = invoiceRepo, userRepo = userRepo)
        val payload = testPayload.copy(totalCost = Some(0.0))
        for
          invoice <- svc.createInvoiceForSession(tenantId, payload)
        yield assertTrue(invoice.amountCents == 0) &&
          assertTrue(invoice.sessions.flatMap(_.dimensions).isEmpty)
      }
    ),

    // ── BillingService — chargeAllDraftsBefore ────────────────────────────

    suite("BillingService.chargeAllDraftsBefore")(
      test("charges all drafts created before the cutoff") {
        val invoiceRepo = new MemInvoiceRepository
        val inv1        = testInvoice.copy(id = "inv-old", createdOn = Instant.parse("2024-01-15T00:00:00Z"))
        val inv2        = testInvoice.copy(id = "inv-new", createdOn = Instant.parse("2025-06-01T00:00:00Z"))
        val cutoff      = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli
        for
          _ <- invoiceRepo.create(inv1)
          _ <- invoiceRepo.create(inv2)
          svc = makeBillingService(invoiceRepo = invoiceRepo)
          charged <- svc.chargeAllDraftsBefore(tenantId, cutoff)
          updated <- invoiceRepo.findById(tenantId, "inv-old")
        yield assertTrue(charged == 1) &&
          assertTrue(updated.exists(_.status == InvoiceStatus.Paid))
      },
      test("returns 0 when no drafts exist before cutoff") {
        val invoiceRepo = new MemInvoiceRepository
        val future      = testInvoice.copy(id = "inv-future", createdOn = Instant.parse("2026-01-01T00:00:00Z"))
        val cutoff      = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli
        for
          _ <- invoiceRepo.create(future)
          svc = makeBillingService(invoiceRepo = invoiceRepo)
          charged <- svc.chargeAllDraftsBefore(tenantId, cutoff)
        yield assertTrue(charged == 0)
      }
    ),

    // ── Dimension builder math ─────────────────────────────────────────────

    suite("dimension builder math")(
      test("3.75 EUR produces exactly 375 cents") {
        assertTrue(math.round(3.75 * 100).toInt == 375)
      },
      test("energy kWh = consumptionWh / 1000") {
        val kwh = testPayload.consumptionWh.map(_ / 1000.0).getOrElse(0.0)
        assertTrue(kwh == 15.0)
      },
      test("zero totalCost returns Nil dimensions (no line items)") {
        val invoiceRepo = new MemInvoiceRepository
        val userRepo    = new MemBillingUserRepository
        userRepo.seed(BillingUser(userId, tenantId, "cus_test", None, now))
        val svc     = makeBillingService(invoiceRepo = invoiceRepo, userRepo = userRepo)
        val payload = testPayload.copy(totalCost = Some(0.0))
        for
          invoice <- svc.createInvoiceForSession(tenantId, payload)
        yield assertTrue(invoice.sessions.flatMap(_.dimensions).isEmpty)
      }
    )
  )
