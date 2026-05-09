package io.itbeans.ev.pricingservice

import zio._

import java.time.{Instant, LocalTime, ZoneId, ZonedDateTime}

// ---------------------------------------------------------------------------
// ConsumptionPricer — prices a single ConsumptionInterval.
//
// Mirrors TypeScript ConsumptionChunkPricer + ConsumptionPricer logic:
//
//   1. For each ≤60s chunk within the interval, dynamic restrictions are
//      checked (day-of-week, time-of-day, energy/duration bounds).
//   2. For each active dimension that passes restrictions, compute amount:
//      - FlatFee:      charged once per session when cumulatedWh > 0
//      - Energy:       (price × consumptionWh) / 1000 [kWh pricing]
//      - ChargingTime: (price × seconds) / 3600 [hourly pricing, active only]
//      - ParkingTime:  (price × seconds) / 3600 [hourly pricing, idle only]
//   3. Step sizes round down to whole billing units before computing.
//
// All arithmetic uses BigDecimal to avoid floating-point drift.
// ---------------------------------------------------------------------------

final class ConsumptionPricer:

  private val MaxChunkSecs = 60

  /**
   * Price one metering interval.
   *
   * @param definition  the resolved tariff for this session
   * @param interval    the consumption data from MeterValues
   * @param state       session state (flat-fee flag, running total)
   * @return  (priced interval result, updated state)
   */
  def priceInterval(
      definition: PricingDefinition,
      interval: ConsumptionInterval,
      state: PricerState
  ): (PricedInterval, PricerState) =
    val chunks = splitIntoChunks(interval)
    val (result, finalState) = chunks.foldLeft((zero(definition, state), state)) {
      case ((acc, st), chunk) => priceChunk(definition, chunk, st, acc)
    }
    (result, finalState)

  // ── Chunk splitting ──────────────────────────────────────────────────────

  private def splitIntoChunks(interval: ConsumptionInterval): List[ConsumptionInterval] =
    val totalSecs = interval.intervalEnd.getEpochSecond - interval.intervalStart.getEpochSecond
    if totalSecs <= MaxChunkSecs then List(interval)
    else
      val numChunks = math.ceil(totalSecs.toDouble / MaxChunkSecs).toInt
      (0 until numChunks).toList.map { i =>
        val chunkStart = interval.intervalStart.plusSeconds(i.toLong * MaxChunkSecs)
        val chunkEnd =
          if i == numChunks - 1 then interval.intervalEnd
          else chunkStart.plusSeconds(MaxChunkSecs)
        val chunkSecs = chunkEnd.getEpochSecond - chunkStart.getEpochSecond
        val ratio     = chunkSecs.toDouble / totalSecs.toDouble
        interval.copy(
          intervalStart = chunkStart,
          intervalEnd = chunkEnd,
          consumptionWh = interval.consumptionWh * ratio,
          inactivitySecs = (interval.inactivitySecs * ratio).toInt,
          // Approximate cumulated values for mid-interval restriction checks
          cumulatedConsumptionWh = interval.cumulatedConsumptionWh -
            interval.consumptionWh * (1.0 - ratio * (i + 1)).max(0.0),
          totalDurationSecs = (interval.totalDurationSecs * ((i + 1).toDouble / numChunks)).toInt
        )
      }

  // ── Per-chunk pricing ────────────────────────────────────────────────────

  private def priceChunk(
      definition: PricingDefinition,
      chunk: ConsumptionInterval,
      state: PricerState,
      acc: PricedInterval
  ): (PricedInterval, PricerState) =
    if !restrictionsMet(definition, chunk) then (acc, state)
    else
      val dims = definition.dimensions
      val flatResult = dims.flatFee.flatMap(d =>
        priceFlatFee(d, chunk.cumulatedConsumptionWh, state.flatFeeAlreadyPriced)
      )
      val energyResult = dims.energy.flatMap(d => priceEnergy(d, chunk.consumptionWh))
      val ctResult     = dims.chargingTime.flatMap(d => priceChargingTime(d, chunk))
      val ptResult     = dims.parkingTime.flatMap(d => priceParkingTime(d, chunk))

      val added        = sum(flatResult) + sum(energyResult) + sum(ctResult) + sum(ptResult)
      val newCumulated = state.cumulatedAmount + added
      val newState = state.copy(
        flatFeeAlreadyPriced = state.flatFeeAlreadyPriced || flatResult.isDefined,
        cumulatedAmount = newCumulated
      )
      val newAcc = acc.copy(
        intervalAmount = acc.intervalAmount + added,
        cumulatedAmount = newCumulated,
        flatFee = acc.flatFee.orElse(flatResult),
        energy = merge(acc.energy, energyResult),
        chargingTime = merge(acc.chargingTime, ctResult),
        parkingTime = merge(acc.parkingTime, ptResult)
      )
      (newAcc, newState)

  private def zero(definition: PricingDefinition, state: PricerState): PricedInterval =
    PricedInterval(
      intervalAmount = BigDecimal(0),
      cumulatedAmount = state.cumulatedAmount,
      currencyCode = definition.currencyCode,
      flatFee = None,
      energy = None,
      chargingTime = None,
      parkingTime = None
    )

  /** Merge two DimensionResults by summing their amounts and quantities. */
  private def merge(a: Option[DimensionResult], b: Option[DimensionResult]): Option[DimensionResult] =
    (a, b) match
      case (None, x) => x
      case (x, None) => x
      case (Some(r1), Some(r2)) =>
        val combined = r1.amount + r2.amount
        Some(r1.copy(
          amount = combined,
          roundedAmount = truncTo2(combined),
          quantity = r1.quantity + r2.quantity
        ))

  private def sum(r: Option[DimensionResult]): BigDecimal =
    r.map(_.amount).getOrElse(BigDecimal(0))

  // ── Dynamic restriction checks ───────────────────────────────────────────

  private def restrictionsMet(d: PricingDefinition, chunk: ConsumptionInterval): Boolean =
    d.restrictions.forall { r =>
      checkDayOfWeek(r, chunk.intervalStart, chunk.timezone) &&
      checkTimeOfDay(r, chunk.intervalStart, chunk.timezone) &&
      checkEnergyBounds(r, chunk.cumulatedConsumptionWh) &&
      checkDurationBounds(r, chunk.totalDurationSecs)
    }

  private def checkDayOfWeek(r: PricingRestriction, ts: Instant, timezone: String): Boolean =
    if r.daysOfWeek.isEmpty then true
    else
      val isoDay =
        try ZonedDateTime.ofInstant(ts, ZoneId.of(timezone)).getDayOfWeek.getValue
        catch case _: Exception => ts.atZone(java.time.ZoneOffset.UTC).getDayOfWeek.getValue
      r.daysOfWeek.exists(_.isoValue == isoDay)

  /**
   * Check that the chunk falls within the [timeFrom, timeTo) window.
   * Handles midnight-crossing ranges (e.g., "20:00" → "08:00").
   */
  private def checkTimeOfDay(r: PricingRestriction, ts: Instant, timezone: String): Boolean =
    (r.timeFrom, r.timeTo) match
      case (None, None) => true
      case _ =>
        val currentTime =
          try ZonedDateTime.ofInstant(ts, ZoneId.of(timezone)).toLocalTime
          catch case _: Exception => ts.atZone(java.time.ZoneOffset.UTC).toLocalTime
        (r.timeFrom, r.timeTo) match
          case (Some(from), Some(to)) =>
            val fromTime = parseTime(from)
            val toTime   = parseTime(to)
            if !fromTime.isAfter(toTime) then
              // Normal range: 08:00–20:00
              !currentTime.isBefore(fromTime) && currentTime.isBefore(toTime)
            else
              // Midnight-crossing range: 20:00–08:00
              !currentTime.isBefore(fromTime) || currentTime.isBefore(toTime)
          case (Some(from), None) => !currentTime.isBefore(parseTime(from))
          case (None, Some(to))   => currentTime.isBefore(parseTime(to))
          case (None, None)       => true

  private def parseTime(s: String): LocalTime =
    LocalTime.parse(s.take(5)) // Accept "HH:mm" or "HH:mm:ss"

  private def checkEnergyBounds(r: PricingRestriction, cumulatedWh: Double): Boolean =
    val kWh = cumulatedWh / 1000.0
    r.minEnergyKwh.forall(min => kWh >= min) &&
    r.maxEnergyKwh.forall(max => kWh < max)

  private def checkDurationBounds(r: PricingRestriction, totalSecs: Int): Boolean =
    r.minDurationSecs.forall(min => totalSecs >= min) &&
      r.maxDurationSecs.forall(max => totalSecs < max)

  // ── Dimension pricing ────────────────────────────────────────────────────

  /**
   * Flat fee: charged once per session when the vehicle has consumed energy.
   * Prevents billing users who plug in but never draw power.
   */
  private def priceFlatFee(
      dim: PricingDimension,
      cumulatedConsumptionWh: Double,
      alreadyPriced: Boolean
  ): Option[DimensionResult] =
    if !dim.active || alreadyPriced || cumulatedConsumptionWh <= 0.0 then None
    else
      Some(DimensionResult(
        amount = dim.price,
        roundedAmount = truncTo2(dim.price),
        quantity = 1.0,
        unitPrice = dim.price,
        stepSize = None
      ))

  /**
   * Energy price: (price × billableWh) / 1000.
   * Step size (Wh) rounds down to whole billing units.
   */
  private def priceEnergy(
      dim: PricingDimension,
      consumptionWh: Double
  ): Option[DimensionResult] =
    if !dim.active || consumptionWh <= 0.0 then None
    else
      val billableWh = dim.stepSize match
        case None       => consumptionWh
        case Some(step) => math.floor(consumptionWh / step) * step
      if billableWh <= 0.0 then None
      else
        val amount = BigDecimal(billableWh) * dim.price / BigDecimal(1000)
        Some(DimensionResult(
          amount = amount,
          roundedAmount = truncTo2(amount),
          quantity = billableWh,
          unitPrice = dim.price,
          stepSize = dim.stepSize
        ))

  /**
   * Charging time: (price × billableSeconds) / 3600.
   * Only priced when consumptionWh > 0 (vehicle actively drawing power).
   * Step size (seconds) rounds down to whole billing units.
   */
  private def priceChargingTime(
      dim: PricingDimension,
      chunk: ConsumptionInterval
  ): Option[DimensionResult] =
    if !dim.active || chunk.consumptionWh <= 0.0 then None
    else
      val chunkSecs = (chunk.intervalEnd.getEpochSecond - chunk.intervalStart.getEpochSecond).toInt
      val billableSecs = dim.stepSize match
        case None       => chunkSecs
        case Some(step) => (chunkSecs / step) * step
      if billableSecs <= 0 then None
      else
        val amount = BigDecimal(billableSecs) * dim.price / BigDecimal(3600)
        Some(DimensionResult(
          amount = amount,
          roundedAmount = truncTo2(amount),
          quantity = billableSecs.toDouble,
          unitPrice = dim.price,
          stepSize = dim.stepSize
        ))

  /**
   * Parking time: (price × billableSeconds) / 3600.
   * Only priced when:
   *   - cumulatedConsumptionWh > 0 (session has started)
   *   - consumptionWh == 0 (currently idle)
   *   - inactivitySecs > 0 (actual idle time in this chunk)
   * Step size (seconds) rounds down to whole billing units.
   */
  private def priceParkingTime(
      dim: PricingDimension,
      chunk: ConsumptionInterval
  ): Option[DimensionResult] =
    if !dim.active then None
    else if chunk.cumulatedConsumptionWh <= 0.0 then None // no charge yet
    else if chunk.consumptionWh > 0.0 then None           // actively charging
    else if chunk.inactivitySecs <= 0 then None           // no idle time
    else
      val billableSecs = dim.stepSize match
        case None       => chunk.inactivitySecs
        case Some(step) => (chunk.inactivitySecs / step) * step
      if billableSecs <= 0 then None
      else
        val amount = BigDecimal(billableSecs) * dim.price / BigDecimal(3600)
        Some(DimensionResult(
          amount = amount,
          roundedAmount = truncTo2(amount),
          quantity = billableSecs.toDouble,
          unitPrice = dim.price,
          stepSize = dim.stepSize
        ))

  // ── Helpers ──────────────────────────────────────────────────────────────

  /** Truncate to 2 decimal places (no rounding up). */
  private def truncTo2(amount: BigDecimal): BigDecimal =
    amount.setScale(2, scala.math.BigDecimal.RoundingMode.FLOOR)

object ConsumptionPricer:

  val live: ZLayer[Any, Nothing, ConsumptionPricer] =
    ZLayer.succeed(new ConsumptionPricer)
