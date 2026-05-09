package io.itbeans.ev.pricingservice

import io.itbeans.ev.domain.TenantId
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.Instant

object PricingServiceSpec extends ZIOSpecDefault:

  // ── Fixtures ──────────────────────────────────────────────────────────────

  private val tenantId     = TenantId("test-tenant")
  private val now          = Instant.parse("2024-06-15T10:00:00Z") // Saturday 10:00 UTC
  private val oneHourLater = now.plusSeconds(3600)

  // Tariff with all four dimensions active, no restrictions
  private val unrestricted = PricingDefinition(
    id = "tariff-1",
    entityId = "tenant-1",
    entityType = PricingEntity.Tenant,
    name = "Standard Tariff",
    description = None,
    siteId = None,
    dimensions = PricingDimensions(
      flatFee = Some(PricingDimension(active = true, price = BigDecimal("1.00"), stepSize = None)),
      energy = Some(PricingDimension(active = true, price = BigDecimal("0.25"), stepSize = None)),
      chargingTime = Some(PricingDimension(active = true, price = BigDecimal("0.05"), stepSize = None)),
      parkingTime = Some(PricingDimension(active = true, price = BigDecimal("0.10"), stepSize = None))
    ),
    staticRestrictions = None,
    restrictions = None,
    currencyCode = "EUR",
    createdOn = now,
    lastChangedOn = now
  )

  // Tariff with energy-only pricing
  private val energyOnly = unrestricted.copy(
    id = "tariff-energy",
    name = "Energy Only",
    dimensions = PricingDimensions(
      flatFee = None,
      energy = Some(PricingDimension(active = true, price = BigDecimal("0.30"), stepSize = None)),
      chargingTime = None,
      parkingTime = None
    )
  )

  // ── In-memory repository stub ─────────────────────────────────────────────

  class MemPricingRepository extends PricingRepository:
    private var store: Map[String, Map[String, PricingDefinition]] = Map.empty

    private def tenantStore(tid: TenantId): Map[String, PricingDefinition] =
      store.getOrElse(tid.value, Map.empty)

    def findByEntity(tid: TenantId, et: PricingEntity, eid: String): Task[List[PricingDefinition]] =
      ZIO.succeed(tenantStore(tid).values.filter(d => d.entityType == et && d.entityId == eid).toList)

    def findByTenant(tid: TenantId): Task[List[PricingDefinition]] =
      ZIO.succeed(tenantStore(tid).values.filter(_.entityType == PricingEntity.Tenant).toList)

    def findById(tid: TenantId, id: String): Task[Option[PricingDefinition]] =
      ZIO.succeed(tenantStore(tid).get(id))

    def upsert(tid: TenantId, d: PricingDefinition): Task[Unit] =
      ZIO.succeed { store = store.updated(tid.value, tenantStore(tid).updated(d.id, d)) }

    def delete(tid: TenantId, id: String): Task[Unit] =
      ZIO.succeed { store = store.updated(tid.value, tenantStore(tid).removed(id)) }

    def findAll(tid: TenantId, limit: Int, skip: Int): Task[List[PricingDefinition]] =
      ZIO.succeed(tenantStore(tid).values.toList.drop(skip).take(limit))

    def countAll(tid: TenantId): Task[Long] =
      ZIO.succeed(tenantStore(tid).size.toLong)

  def makeResolver(defs: List[PricingDefinition]): Task[TariffResolver] =
    for
      repo <- ZIO.succeed(new MemPricingRepository)
      _    <- ZIO.foreach(defs)(d => repo.upsert(tenantId, d))
    yield new TariffResolver(repo)

  private val pricer = new ConsumptionPricer

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def interval(
      consumptionWh: Double,
      cumulatedWh: Double,
      durationSecs: Int = 60,
      inactivitySecs: Int = 0,
      totalInactivitySecs: Int = 0,
      start: Instant = now,
      timezone: String = "UTC"
  ): ConsumptionInterval = ConsumptionInterval(
    intervalStart = start,
    intervalEnd = start.plusSeconds(durationSecs),
    consumptionWh = consumptionWh,
    cumulatedConsumptionWh = cumulatedWh,
    totalDurationSecs = durationSecs,
    inactivitySecs = inactivitySecs,
    totalInactivitySecs = totalInactivitySecs,
    timezone = timezone,
    connectorType = "Type2",
    connectorPowerKw = 11.0
  )

  // ── Test suites ───────────────────────────────────────────────────────────

  override def spec = suite("ev-pricing")(
    suite("ConsumptionPricer — flat fee")(
      test("charges flat fee once when cumulated consumption > 0") {
        val state0           = PricerState.initial("EUR")
        val i                = interval(consumptionWh = 1000, cumulatedWh = 1000)
        val (priced, state1) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.flatFee.isDefined) &&
        assertTrue(priced.flatFee.get.amount == BigDecimal("1.00")) &&
        assertTrue(state1.flatFeeAlreadyPriced)
      },
      test("does not charge flat fee when cumulated consumption is 0") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 0, cumulatedWh = 0)
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.flatFee.isEmpty)
      },
      test("does not charge flat fee twice across two intervals") {
        val state0      = PricerState.initial("EUR")
        val i1          = interval(consumptionWh = 1000, cumulatedWh = 1000)
        val i2          = interval(consumptionWh = 1000, cumulatedWh = 2000, start = now.plusSeconds(60))
        val (_, state1) = pricer.priceInterval(unrestricted, i1, state0)
        val (p2, _)     = pricer.priceInterval(unrestricted, i2, state1)
        assertTrue(p2.flatFee.isEmpty) // already priced in interval 1
      }
    ),
    suite("ConsumptionPricer — energy")(
      test("energy price = (unitPrice * Wh) / 1000") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 10_000, cumulatedWh = 10_000)
        val (priced, _) = pricer.priceInterval(energyOnly, i, state0)
        // 10 kWh × 0.30 €/kWh = 3.00 €
        assertTrue(priced.energy.isDefined) &&
        assertTrue(priced.energy.get.amount == BigDecimal("3.00"))
      },
      test("energy with step size floors to whole billing units") {
        val tariffWithStep = energyOnly.copy(
          dimensions = energyOnly.dimensions.copy(
            energy =
              Some(PricingDimension(active = true, price = BigDecimal("0.30"), stepSize = Some(1000))) // 1 kWh steps
          )
        )
        val state0 = PricerState.initial("EUR")
        // 1500 Wh → floors to 1000 Wh (1 complete step)
        val i           = interval(consumptionWh = 1500, cumulatedWh = 1500)
        val (priced, _) = pricer.priceInterval(tariffWithStep, i, state0)
        // 1 kWh × 0.30 = 0.30
        assertTrue(priced.energy.get.quantity == 1000.0) &&
        assertTrue(priced.energy.get.amount == BigDecimal("0.30") / BigDecimal("1000") * BigDecimal("1000"))
      },
      test("no energy charge when consumptionWh is 0") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 0, cumulatedWh = 5000)
        val (priced, _) = pricer.priceInterval(energyOnly, i, state0)
        assertTrue(priced.energy.isEmpty)
      }
    ),
    suite("ConsumptionPricer — charging time")(
      test("charging time priced when actively consuming") {
        val state0 = PricerState.initial("EUR")
        // 60-second interval, 0.05 €/h → 60/3600 × 0.05 = 0.000833...
        val i           = interval(consumptionWh = 1000, cumulatedWh = 1000, durationSecs = 60)
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.chargingTime.isDefined) &&
        assertTrue(priced.chargingTime.get.quantity == 60.0)
      },
      test("no charging time when consumptionWh is 0") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 0, cumulatedWh = 5000, inactivitySecs = 60)
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.chargingTime.isEmpty)
      }
    ),
    suite("ConsumptionPricer — parking time")(
      test("parking time priced when idle after charging") {
        val state0 = PricerState.initial("EUR")
        val i = interval(
          consumptionWh = 0,
          cumulatedWh = 10_000, // session has charged
          inactivitySecs = 300,
          totalInactivitySecs = 300
        )
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.parkingTime.isDefined) &&
        assertTrue(priced.parkingTime.get.quantity == 300.0)
      },
      test("no parking time when no prior energy delivered") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 0, cumulatedWh = 0, inactivitySecs = 300)
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.parkingTime.isEmpty)
      },
      test("no parking time when consumptionWh > 0 (actively charging)") {
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 500, cumulatedWh = 5500, inactivitySecs = 30)
        val (priced, _) = pricer.priceInterval(unrestricted, i, state0)
        assertTrue(priced.parkingTime.isEmpty)
      }
    ),
    suite("ConsumptionPricer — dynamic restrictions")(
      test("time-of-day restriction: prices within window") {
        // Tariff valid 08:00–20:00 UTC
        val tariff = unrestricted.copy(
          restrictions = Some(PricingRestriction(
            daysOfWeek = Nil,
            timeFrom = Some("08:00"),
            timeTo = Some("20:00"),
            minEnergyKwh = None,
            maxEnergyKwh = None,
            minDurationSecs = None,
            maxDurationSecs = None
          ))
        )
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 1000, cumulatedWh = 1000, start = now) // 10:00 UTC
        val (priced, _) = pricer.priceInterval(tariff, i, state0)
        assertTrue(priced.energy.isDefined)
      },
      test("time-of-day restriction: zero price outside window") {
        // Tariff valid 20:00–08:00 (night rate), but interval is at 10:00
        val tariff = unrestricted.copy(
          restrictions = Some(PricingRestriction(
            daysOfWeek = Nil,
            timeFrom = Some("20:00"),
            timeTo = Some("08:00"),
            minEnergyKwh = None,
            maxEnergyKwh = None,
            minDurationSecs = None,
            maxDurationSecs = None
          ))
        )
        val state0 = PricerState.initial("EUR")
        val i      = interval(consumptionWh = 1000, cumulatedWh = 1000, start = now) // 10:00 UTC — outside night window
        val (priced, _) = pricer.priceInterval(tariff, i, state0)
        assertTrue(priced.energy.isEmpty) &&
        assertTrue(priced.intervalAmount == BigDecimal(0))
      },
      test("day-of-week restriction: prices on matching day") {
        // now is Saturday (isoWeekday = 6)
        val tariff = unrestricted.copy(
          restrictions = Some(PricingRestriction(
            daysOfWeek = List(DayOfWeek.Saturday),
            timeFrom = None,
            timeTo = None,
            minEnergyKwh = None,
            maxEnergyKwh = None,
            minDurationSecs = None,
            maxDurationSecs = None
          ))
        )
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 1000, cumulatedWh = 1000)
        val (priced, _) = pricer.priceInterval(tariff, i, state0)
        assertTrue(priced.energy.isDefined)
      },
      test("day-of-week restriction: zero price on non-matching day") {
        // now is Saturday; tariff only applies on weekdays
        val tariff = unrestricted.copy(
          restrictions = Some(PricingRestriction(
            daysOfWeek =
              List(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday),
            timeFrom = None,
            timeTo = None,
            minEnergyKwh = None,
            maxEnergyKwh = None,
            minDurationSecs = None,
            maxDurationSecs = None
          ))
        )
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 1000, cumulatedWh = 1000) // Saturday
        val (priced, _) = pricer.priceInterval(tariff, i, state0)
        assertTrue(priced.energy.isEmpty) &&
        assertTrue(priced.intervalAmount == BigDecimal(0))
      },
      test("min energy restriction: not triggered below threshold") {
        val tariff = unrestricted.copy(
          restrictions = Some(PricingRestriction(
            daysOfWeek = Nil,
            timeFrom = None,
            timeTo = None,
            minEnergyKwh = Some(5.0), // only applies after 5 kWh
            maxEnergyKwh = None,
            minDurationSecs = None,
            maxDurationSecs = None
          ))
        )
        val state0      = PricerState.initial("EUR")
        val i           = interval(consumptionWh = 1000, cumulatedWh = 2000) // 2 kWh < 5 kWh threshold
        val (priced, _) = pricer.priceInterval(tariff, i, state0)
        assertTrue(priced.intervalAmount == BigDecimal(0))
      }
    ),
    suite("TariffResolver — static restrictions")(
      test("selects tenant tariff when no other matches") {
        for
          resolver <- makeResolver(List(unrestricted))
          ctx = TariffResolver.ResolutionContext(
            chargingStationId = "cs-1",
            siteAreaId = None,
            siteId = None,
            companyId = None,
            tenantId = tenantId,
            connectorType = "Type2",
            connectorPowerKw = 11.0,
            timestamp = now
          )
          result <- resolver.resolve(ctx)
        yield assertTrue(result.isDefined) && assertTrue(result.get.id == "tariff-1")
      },
      test("static restriction: excludes expired tariff") {
        val expired = unrestricted.copy(
          id = "expired",
          staticRestrictions = Some(PricingStaticRestriction(
            validFrom = None,
            validTo = Some(Instant.parse("2024-01-01T00:00:00Z")), // expired
            connectorType = None,
            connectorPowerKw = None
          ))
        )
        for
          resolver <- makeResolver(List(expired))
          ctx = TariffResolver.ResolutionContext(
            chargingStationId = "cs-1",
            siteAreaId = None,
            siteId = None,
            companyId = None,
            tenantId = tenantId,
            connectorType = "Type2",
            connectorPowerKw = 11.0,
            timestamp = now
          )
          result <- resolver.resolve(ctx)
        yield assertTrue(result.isEmpty) // expired tariff not selected
      },
      test("static restriction: connector type mismatch excludes tariff") {
        val ccsOnly = unrestricted.copy(
          id = "ccs-only",
          staticRestrictions = Some(PricingStaticRestriction(
            validFrom = None,
            validTo = None,
            connectorType = Some("CCS"),
            connectorPowerKw = None
          ))
        )
        for
          resolver <- makeResolver(List(ccsOnly))
          ctx = TariffResolver.ResolutionContext(
            chargingStationId = "cs-1",
            siteAreaId = None,
            siteId = None,
            companyId = None,
            tenantId = tenantId,
            connectorType = "Type2", // doesn't match CCS
            connectorPowerKw = 11.0,
            timestamp = now
          )
          result <- resolver.resolve(ctx)
        yield assertTrue(result.isEmpty)
      },
      test("more specific (station) tariff wins over tenant tariff") {
        val stationTariff = unrestricted.copy(
          id = "station-tariff",
          entityId = "station-1",
          entityType = PricingEntity.ChargingStation,
          name = "Station Special Rate"
        )
        for
          resolver <- makeResolver(List(unrestricted, stationTariff))
          ctx = TariffResolver.ResolutionContext(
            chargingStationId = "station-1",
            siteAreaId = None,
            siteId = None,
            companyId = None,
            tenantId = tenantId,
            connectorType = "Type2",
            connectorPowerKw = 11.0,
            timestamp = now
          )
          result <- resolver.resolve(ctx)
        yield assertTrue(result.isDefined) && assertTrue(result.get.id == "station-tariff")
      }
    ),
    suite("PricingRepository CRUD")(
      test("upsert and retrieve by id") {
        val repo = new MemPricingRepository
        for
          _       <- repo.upsert(tenantId, unrestricted)
          fetched <- repo.findById(tenantId, unrestricted.id)
        yield assertTrue(fetched.isDefined) &&
          assertTrue(fetched.get.name == "Standard Tariff")
      },
      test("delete removes the definition") {
        val repo = new MemPricingRepository
        for
          _       <- repo.upsert(tenantId, unrestricted)
          _       <- repo.delete(tenantId, unrestricted.id)
          fetched <- repo.findById(tenantId, unrestricted.id)
        yield assertTrue(fetched.isEmpty)
      },
      test("countAll reflects inserts") {
        val repo = new MemPricingRepository
        for
          _     <- repo.upsert(tenantId, unrestricted)
          _     <- repo.upsert(tenantId, energyOnly)
          count <- repo.countAll(tenantId)
        yield assertTrue(count == 2L)
      }
    ),
    suite("PricingDomain — Circe codecs")(
      test("PricingEntity round-trips through JSON") {
        import io.circe.syntax._
        import io.circe.parser.decode
        import CirceInstances.given
        val encoded = PricingEntity.ChargingStation.asJson.noSpaces
        val decoded = decode[PricingEntity](encoded)
        assertTrue(encoded == "\"CS\"") && assertTrue(decoded == Right(PricingEntity.ChargingStation))
      },
      test("DayOfWeek round-trips through JSON") {
        import io.circe.syntax._
        import io.circe.parser.decode
        import CirceInstances.given
        val encoded = DayOfWeek.Saturday.asJson.noSpaces
        val decoded = decode[DayOfWeek](encoded)
        assertTrue(encoded == "6") && assertTrue(decoded == Right(DayOfWeek.Saturday))
      },
      test("PricingDefinition serialises and deserialises correctly") {
        import io.circe.syntax._
        import io.circe.parser.decode
        import CirceInstances.given
        val json    = unrestricted.asJson.noSpaces
        val decoded = decode[PricingDefinition](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.get.name == unrestricted.name) &&
        assertTrue(decoded.toOption.get.currencyCode == "EUR")
      }
    )
  )
