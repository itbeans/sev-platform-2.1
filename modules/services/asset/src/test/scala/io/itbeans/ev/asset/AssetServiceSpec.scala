package io.itbeans.ev.asset

import io.itbeans.ev.asset.connector.{AssetConnector, AssetConnectorRegistry}
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.kafka.EvKafkaProducer
import io.circe.Encoder
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant
import java.util.UUID

object AssetServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val tenantId = TenantId("test-tenant")
  private val now      = Instant.parse("2024-06-15T10:00:00Z")

  private def makeAsset(
      id: String = UUID.randomUUID().toString,
      dynamic: Boolean = false,
      usesPushApi: Boolean = false,
      connectionId: Option[String] = None,
      meterId: Option[String] = None,
      lastTs: Option[Instant] = None
  ): Asset = Asset(
    id = id,
    name = s"Asset $id",
    companyId = "company-1",
    siteId = Some("site-1"),
    siteAreaId = "area-1",
    assetType = AssetType.Consumption,
    fluctuationPercent = 10.0,
    staticValueWatt = 1000.0,
    coordinates = List(2.35, 48.85),
    issuer = true,
    image = None,
    dynamicAsset = dynamic,
    usesPushApi = usesPushApi,
    connectionId = connectionId,
    meterId = meterId,
    excludeFromSmartCharging = false,
    variationThresholdPercent = None,
    powerWattsLastSmartCharging = None,
    currentConsumption = AssetCurrentConsumption.zero.copy(lastConsumptionTimestamp = lastTs),
    createdOn = now,
    lastChangedOn = now
  )

  private def makeMeasurement(ts: Instant, watts: Double, wh: Double): AssetMeasurement =
    AssetMeasurement(
      timestamp = ts,
      instantWatts = watts,
      instantWattsL1 = None,
      instantWattsL2 = None,
      instantWattsL3 = None,
      instantAmps = None,
      instantAmpsL1 = None,
      instantAmpsL2 = None,
      instantAmpsL3 = None,
      instantVolts = None,
      instantVoltsL1 = None,
      instantVoltsL2 = None,
      instantVoltsL3 = None,
      consumptionWh = wh,
      stateOfCharge = None
    )

  private val testConnection = AssetConnection(
    id = "conn-1",
    name = "Test Connection",
    connectorType = AssetConnectorType.Greencom,
    url = "https://api.test.example.com",
    refreshIntervalMins = 5,
    clientId = Some("client-id"),
    clientSecret = Some("client-secret"),
    authenticationUrl = None,
    user = None,
    password = None
  )

  // ── In-memory stubs ────────────────────────────────────────────────────────

  class MemAssetRepository extends AssetRepository:
    private var store: Map[String, Asset] = Map.empty

    def upsert(tenantId: TenantId, asset: Asset): Task[Unit] =
      ZIO.succeed { store = store.updated(asset.id, asset) }

    def findById(tenantId: TenantId, assetId: String): Task[Option[Asset]] =
      ZIO.succeed(store.get(assetId))

    def findAll(tenantId: TenantId, dynamicOnly: Boolean, limit: Int, skip: Int): Task[List[Asset]] =
      ZIO.succeed(
        store.values.toList
          .filter(a => !dynamicOnly || a.dynamicAsset)
          .sortBy(_.name)
          .drop(skip)
          .take(limit)
      )

    def countAll(tenantId: TenantId, dynamicOnly: Boolean): Task[Long] =
      ZIO.succeed(store.values.count(a => !dynamicOnly || a.dynamicAsset).toLong)

    def delete(tenantId: TenantId, assetId: String): Task[Unit] =
      ZIO.succeed { store = store.removed(assetId) }

    def getAll: Map[String, Asset] = store

  class MemConnectionRepository(connections: Map[String, AssetConnection]) extends AssetConnectionRepository:

    def findById(tenantId: TenantId, connectionId: String): Task[Option[AssetConnection]] =
      ZIO.succeed(connections.get(connectionId))

    def upsert(tenantId: TenantId, connection: AssetConnection): Task[Unit] = ZIO.unit
    def delete(tenantId: TenantId, connectionId: String): Task[Unit]        = ZIO.unit

  class FakeConnector(measurements: List[AssetMeasurement]) extends AssetConnector:
    def connectorType: AssetConnectorType = AssetConnectorType.Greencom

    def fetchConsumptions(
        connection: AssetConnection,
        meterId: String,
        assetType: AssetType,
        since: Option[Instant],
        manualCall: Boolean
    ): Task[List[AssetMeasurement]] = ZIO.succeed(measurements)

  class CapturingKafka extends EvKafkaProducer:
    private var published: List[(String, String)] = Nil

    def publish[A: Encoder](topic: String, partitionKey: String, event: A): Task[Unit] =
      ZIO.succeed { published = published :+ (topic, partitionKey) }

    def publishOcppEvent[A: Encoder](tenantId: TenantId, partitionKey: String, event: A): Task[Unit] =
      ZIO.unit

    def events: List[(String, String)] = published

  // ── Tests ─────────────────────────────────────────────────────────────────

  override def spec = suite("AssetServiceSpec")(
    // ── AssetRepository ─────────────────────────────────────────────────────

    suite("MemAssetRepository")(
      test("upsert and findById round-trip") {
        val repo  = new MemAssetRepository
        val asset = makeAsset(id = "asset-1")
        for
          _   <- repo.upsert(tenantId, asset)
          got <- repo.findById(tenantId, "asset-1")
        yield assertTrue(got.contains(asset))
      },
      test("findAll with dynamicOnly=true returns only dynamic assets") {
        val repo    = new MemAssetRepository
        val static  = makeAsset(id = "s1", dynamic = false)
        val dynamic = makeAsset(id = "d1", dynamic = true)
        for
          _   <- repo.upsert(tenantId, static)
          _   <- repo.upsert(tenantId, dynamic)
          all <- repo.findAll(tenantId, dynamicOnly = false, limit = 100, skip = 0)
          dyn <- repo.findAll(tenantId, dynamicOnly = true, limit = 100, skip = 0)
        yield assertTrue(all.length == 2) && assertTrue(dyn == List(dynamic))
      },
      test("countAll with dynamicOnly=false counts all assets") {
        val repo = new MemAssetRepository
        for
          _ <- repo.upsert(tenantId, makeAsset(id = "a1", dynamic = false))
          _ <- repo.upsert(tenantId, makeAsset(id = "a2", dynamic = true))
          n <- repo.countAll(tenantId, dynamicOnly = false)
        yield assertTrue(n == 2L)
      },
      test("delete removes the asset") {
        val repo  = new MemAssetRepository
        val asset = makeAsset(id = "del-1")
        for
          _   <- repo.upsert(tenantId, asset)
          _   <- repo.delete(tenantId, "del-1")
          got <- repo.findById(tenantId, "del-1")
        yield assertTrue(got.isEmpty)
      },
      test("findAll supports pagination (skip + limit)") {
        val repo = new MemAssetRepository
        for
          _ <- ZIO.foreachDiscard(1 to 5)(i => repo.upsert(tenantId, makeAsset(id = s"p$i")))
          p <- repo.findAll(tenantId, dynamicOnly = false, limit = 2, skip = 2)
        yield assertTrue(p.length == 2)
      }
    ),

    // ── AssetDomain ─────────────────────────────────────────────────────────

    suite("AssetDomain")(
      test("AssetType round-trips through fromCode") {
        val types = AssetType.values.toList
        assertTrue(types.forall(t => AssetType.fromCode(t.code).contains(t)))
      },
      test("AssetConnectorType round-trips through fromCode") {
        val types = AssetConnectorType.values.toList
        assertTrue(types.forall(t => AssetConnectorType.fromCode(t.code).contains(t)))
      },
      test("AssetCurrentConsumption.zero has all-zero baseline") {
        val z = AssetCurrentConsumption.zero
        assertTrue(z.currentInstantWatts == 0.0) &&
        assertTrue(z.currentTotalConsumptionWh == 0.0) &&
        assertTrue(z.lastConsumptionTimestamp.isEmpty)
      },
      test("AssetType.fromCode returns None for unknown code") {
        assertTrue(AssetType.fromCode("INVALID").isEmpty)
      }
    ),

    // ── AssetGetConsumptionTask ──────────────────────────────────────────────

    suite("AssetGetConsumptionTask")(
      test("skips static assets (dynamicAsset=false)") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map.empty)
        val kafka    = new CapturingKafka
        val fake     = new FakeConnector(List(makeMeasurement(now, 500.0, 100.0)))
        val registry = new AssetConnectorRegistry(Map(AssetConnectorType.Greencom -> fake))
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        // Only a static asset in the repo
        val static = makeAsset(id = "s1", dynamic = false, connectionId = Some("conn-1"), meterId = Some("m1"))
        for
          _ <- repo.upsert(tenantId, static)
          _ <- task.run(tenantId)
        yield assertTrue(kafka.events.isEmpty) // no polling, no Kafka publish
      },
      test("polls dynamic assets and publishes to Kafka") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map("conn-1" -> testConnection))
        val kafka    = new CapturingKafka
        val m        = makeMeasurement(now, 2000.0, 500.0)
        val fake     = new FakeConnector(List(m))
        val registry = new AssetConnectorRegistry(Map(AssetConnectorType.Greencom -> fake))
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        val dynAsset = makeAsset(id = "dyn-1", dynamic = true, connectionId = Some("conn-1"), meterId = Some("meter-1"))
        for
          _      <- repo.upsert(tenantId, dynAsset)
          _      <- task.run(tenantId)
          stored <- repo.findById(tenantId, "dyn-1")
        yield assertTrue(kafka.events.length == 1) &&
          assertTrue(kafka.events.head._1 == "asset.consumptions") &&
          assertTrue(kafka.events.head._2 == "dyn-1") &&
          assertTrue(stored.map(_.currentConsumption.currentInstantWatts).contains(2000.0))
      },
      test("skips dynamic asset without connectionId") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map.empty)
        val kafka    = new CapturingKafka
        val registry = new AssetConnectorRegistry(Map.empty)
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        val noConn = makeAsset(id = "nc-1", dynamic = true, connectionId = None, meterId = Some("m1"))
        for
          _ <- repo.upsert(tenantId, noConn)
          _ <- task.run(tenantId)
        yield assertTrue(kafka.events.isEmpty)
      },
      test("skips dynamic asset without meterId") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map("conn-1" -> testConnection))
        val kafka    = new CapturingKafka
        val registry = new AssetConnectorRegistry(Map.empty)
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        val noMeter = makeAsset(id = "nm-1", dynamic = true, connectionId = Some("conn-1"), meterId = None)
        for
          _ <- repo.upsert(tenantId, noMeter)
          _ <- task.run(tenantId)
        yield assertTrue(kafka.events.isEmpty)
      },
      test("dedup filters measurements within 50s of last timestamp") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map("conn-1" -> testConnection))
        val kafka    = new CapturingKafka
        // Two measurements: one 30s after last (filtered), one 60s after last (kept)
        val lastTs   = now
        val tooSoon  = makeMeasurement(lastTs.plusSeconds(30), 500.0, 50.0)
        val ok       = makeMeasurement(lastTs.plusSeconds(60), 1000.0, 100.0)
        val fake     = new FakeConnector(List(tooSoon, ok))
        val registry = new AssetConnectorRegistry(Map(AssetConnectorType.Greencom -> fake))
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        val asset = makeAsset(
          id = "dedup-1",
          dynamic = true,
          connectionId = Some("conn-1"),
          meterId = Some("m1"),
          lastTs = Some(lastTs)
        )
        for
          _ <- repo.upsert(tenantId, asset)
          _ <- task.run(tenantId)
        // Only the 60s measurement should have been published
        yield assertTrue(kafka.events.length == 1)
      },
      test("accumulates currentTotalConsumptionWh across multiple measurements") {
        val repo     = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map("conn-1" -> testConnection))
        val kafka    = new CapturingKafka
        val m1       = makeMeasurement(now, 500.0, 100.0)
        val m2       = makeMeasurement(now.plusSeconds(60), 600.0, 120.0)
        val fake     = new FakeConnector(List(m1, m2))
        val registry = new AssetConnectorRegistry(Map(AssetConnectorType.Greencom -> fake))
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, kafka)

        val asset = makeAsset(id = "acc-1", dynamic = true, connectionId = Some("conn-1"), meterId = Some("m1"))
        for
          _      <- repo.upsert(tenantId, asset)
          _      <- task.run(tenantId)
          stored <- repo.findById(tenantId, "acc-1")
        yield assertTrue(stored.map(_.currentConsumption.currentTotalConsumptionWh).contains(220.0))
      },
      test("Kafka publish failure does not abort task for remaining assets") {
        val repo = new MemAssetRepository
        val connRepo = new MemConnectionRepository(Map(
          "conn-1" -> testConnection,
          "conn-2" -> testConnection.copy(id = "conn-2")
        ))
        val failOnce = new EvKafkaProducer:
          private var callCount = 0
          def publish[A: Encoder](topic: String, key: String, event: A): Task[Unit] =
            ZIO.succeed {
              callCount += 1
            } *>
              (if callCount == 1 then ZIO.fail(new Exception("Kafka down")) else ZIO.unit)
          def publishOcppEvent[A: Encoder](tid: TenantId, key: String, event: A): Task[Unit] = ZIO.unit

        val fake     = new FakeConnector(List(makeMeasurement(now, 100.0, 10.0)))
        val registry = new AssetConnectorRegistry(Map(AssetConnectorType.Greencom -> fake))
        val task     = new AssetGetConsumptionTask(repo, connRepo, registry, failOnce)

        val a1 = makeAsset(id = "k1", dynamic = true, connectionId = Some("conn-1"), meterId = Some("m1"))
        val a2 = makeAsset(id = "k2", dynamic = true, connectionId = Some("conn-2"), meterId = Some("m2"))
        for
          _ <- repo.upsert(tenantId, a1)
          _ <- repo.upsert(tenantId, a2)
          _ <- task.run(tenantId)
          // Both assets should have been polled: a2's consumption recorded even if a1's Kafka publish failed
          s2 <- repo.findById(tenantId, "k2")
        yield assertTrue(s2.map(_.currentConsumption.currentInstantWatts).contains(100.0))
      }
    ),

    // ── AssetConnectionRepository ────────────────────────────────────────────

    suite("MemConnectionRepository")(
      test("findById returns Some for known connection") {
        val repo = new MemConnectionRepository(Map("conn-1" -> testConnection))
        for got <- repo.findById(tenantId, "conn-1")
        yield assertTrue(got.contains(testConnection))
      },
      test("findById returns None for unknown connection") {
        val repo = new MemConnectionRepository(Map.empty)
        for got <- repo.findById(tenantId, "missing")
        yield assertTrue(got.isEmpty)
      }
    )
  )
