package io.itbeans.ev.smartcharging

import zio._

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// SmartChargingService — core orchestration for SAP Smart Charging.
//
// Flow triggered per siteAreaId:
//   1. Load SiteAreaSC + active transactions from MongoDB
//   2. Build OptimizerRequest (fuse tree + cars + assignments)
//   3. POST to SAP Smart Charging Optimizer
//   4. Convert optimizer car plans → ChargingProfile documents
//   5. Persist profiles to MongoDB (upsert)
//   6. (Future) gRPC to ev-ocpp-gateway: SetChargingProfile per station
// ---------------------------------------------------------------------------

trait SmartChargingService:
  /** Compute, persist, and deliver profiles. Returns (profilesApplied, profilesFailed). */
  def triggerOptimization(tenantId: String, siteAreaId: String, event: String): Task[(Int, Int)]

  /** Compute profiles without persisting or delivering (dry-run). */
  def buildChargingProfiles(
      tenantId: String,
      siteAreaId: String,
      excludedStationIds: List[String]
  ): Task[List[ChargingProfile]]

final class LiveSmartChargingService(
    repo: SmartChargingRepository,
    optimizer: SapSmartChargingClient,
    gatewayClient: SmartChargingGatewayClient,
    cfg: SmartChargingConfig
) extends SmartChargingService:

  def triggerOptimization(tenantId: String, siteAreaId: String, event: String): Task[(Int, Int)] =
    for
      siteAreaOpt <- repo.findSiteArea(tenantId, siteAreaId)
      siteArea <- ZIO.fromOption(siteAreaOpt)
        .orElseFail(new Exception(s"SiteArea $siteAreaId not found for tenant $tenantId"))
      _ <- ZIO.logInfo(
        s"[SmartCharging] Optimizing siteArea=$siteAreaId event=$event " +
          s"maxPower=${siteArea.maximumPower}W"
      )
      txs <- repo.findActiveTransactions(tenantId, siteAreaId)
      _   <- ZIO.logDebug(s"[SmartCharging] Active transactions: ${txs.size}")
      request = buildRequest(siteArea, txs, event)
      result <- optimizer.optimize(request)
        .tapError(err =>
          ZIO.logError(
            s"[SmartCharging] SAP optimizer call failed for $siteAreaId: ${err.getMessage}"
          )
        )
      profiles = buildProfiles(tenantId, siteAreaId, siteArea.siteId, txs, result, siteArea)
      _ <- ZIO.foreach(profiles)(repo.saveProfile)
      deliveryResults <- ZIO.foreach(profiles) { profile =>
        gatewayClient.deliverChargingProfile(tenantId, profile)
          .tapError(err =>
            ZIO.logWarning(
              s"[SmartCharging] Failed to deliver profile to ${profile.chargingStationId}: ${err.getMessage}"
            )
          )
          .fold(_ => false, identity)
      }
      applied = deliveryResults.count(identity)
      failed  = deliveryResults.count(!_)
      _ <- ZIO.logInfo(
        s"[SmartCharging] Applied $applied, failed $failed charging profiles for $siteAreaId"
      )
    yield (applied, failed)

  def buildChargingProfiles(
      tenantId: String,
      siteAreaId: String,
      excludedStationIds: List[String]
  ): Task[List[ChargingProfile]] =
    for
      siteAreaOpt <- repo.findSiteArea(tenantId, siteAreaId)
      siteArea <- ZIO.fromOption(siteAreaOpt)
        .orElseFail(new Exception(s"SiteArea $siteAreaId not found for tenant $tenantId"))
      txs <- repo.findActiveTransactions(tenantId, siteAreaId)
      filteredTxs = txs.filterNot(tx => excludedStationIds.contains(tx.chargingStationId))
      request = buildRequest(siteArea, filteredTxs, "Reoptimize")
      result  <- optimizer.optimize(request)
    yield buildProfiles(tenantId, siteAreaId, siteArea.siteId, filteredTxs, result, siteArea)

  // ── Build OptimizerRequest ────────────────────────────────────────────

  private def buildRequest(
      siteArea: SiteAreaSC,
      txs: List[ActiveTransactionSC],
      event: String
  ): OptimizerRequest =
    val voltage   = siteArea.voltage.getOrElse(cfg.defaultVoltage).toDouble
    val numPhases = siteArea.numberOfPhases.getOrElse(cfg.defaultNumberPhases)
    val gridFuseAmps = siteArea.maximumPower / (voltage * numPhases)

    // Group transactions by charging station to build per-station child fuses.
    // Station fuse id starts at 1; car ids align with transaction index (1-based).
    val txByStation: Map[String, List[ActiveTransactionSC]] =
      txs.groupBy(_.chargingStationId)

    // Assign a stable fuse ID to each station (deterministic by sorted station ID)
    val stationFuseIds: Map[String, Int] =
      txByStation.keys.toList.sorted.zipWithIndex.map { case (sid, idx) => sid -> (idx + 1) }.toMap

    // Build one child OptimizerFuse per station.
    // Amperage = sum of max connector amperage across all active transactions on that station.
    val stationFuses: Map[String, OptimizerFuse] =
      stationFuseIds.map { case (stationId, fuseId) =>
        val stationTxs = txByStation(stationId)
        val stationAmps = stationTxs.map(_.maxCurrentA).sum.max(gridFuseAmps / stationFuseIds.size)
        stationId -> OptimizerFuse(
          id = fuseId,
          fusePhase1 = stationAmps,
          fusePhase2 = stationAmps,
          fusePhase3 = stationAmps
        )
      }

    // Root fuse contains all station child fuses
    val rootFuse = OptimizerFuse(
      id = 0,
      fusePhase1 = gridFuseAmps,
      fusePhase2 = gridFuseAmps,
      fusePhase3 = gridFuseAmps,
      children = stationFuses.values.toList.sortBy(_.id)
    )

    val cars = txs.zipWithIndex.map { case (tx, idx) => buildCar(idx + 1, tx) }

    // Each car is assigned to its station's child fuse (not the root)
    val carAssignments = txs.zipWithIndex.map { case (tx, idx) =>
      CarAssignment(carID = idx + 1, chargingStationID = stationFuseIds(tx.chargingStationId))
    }

    val nowSecs            = java.lang.System.currentTimeMillis() / 1000
    val currentTimeSeconds = (nowSecs % 900).toInt

    OptimizerRequest(
      event = OptimizerEvent(eventType = event),
      state = OptimizerState(
        fuseTree = Map("0" -> rootFuse),
        cars = cars,
        carAssignments = carAssignments,
        currentTimeSeconds = currentTimeSeconds
      )
    )

  private def buildCar(id: Int, tx: ActiveTransactionSC): OptimizerCar =
    // Estimate current energy from SoC if available; otherwise assume 50% of capacity
    val estimatedCapacity = 60.0 // kWh — default if we don't know the car
    val currentEnergy     = tx.stateOfCharge.map(_ / 100.0 * estimatedCapacity).getOrElse(estimatedCapacity * 0.5)

    OptimizerCar(
      id = id,
      name = s"${tx.chargingStationId}~${tx.connectorId}",
      carType = "BEV",
      startCapacity = currentEnergy,
      maxCapacity = estimatedCapacity,
      minCurrentPerPhase = 6.0,
      maxCurrentPerPhase = tx.maxCurrentA,
      canLoadPhase1 = 1,
      canLoadPhase2 = if tx.numberOfPhases >= 2 then 1 else 0,
      canLoadPhase3 = if tx.numberOfPhases >= 3 then 1 else 0,
      currentPlan = Nil // filled in by the optimizer in its response
    )

  // ── Convert optimizer result → ChargingProfiles ───────────────────────

  private def buildProfiles(
      tenantId: String,
      siteAreaId: String,
      siteId: Option[String],
      txs: List[ActiveTransactionSC],
      result: OptimizerResult,
      siteArea: SiteAreaSC
  ): List[ChargingProfile] =
    val txByCarId: Map[Int, ActiveTransactionSC] =
      txs.zipWithIndex.map { case (tx, idx) => (idx + 1) -> tx }.toMap

    val voltage = siteArea.voltage.getOrElse(cfg.defaultVoltage).toDouble
    val now     = Instant.now()
    // Align start to the last 15-minute mark
    val epochMs       = now.toEpochMilli
    val slotMs        = 15 * 60 * 1000L
    val scheduleStart = Instant.ofEpochMilli(epochMs - (epochMs % slotMs))

    result.cars.flatMap { car =>
      txByCarId.get(car.id).map { tx =>
        val numPhases = tx.numberOfPhases
        // Convert amperage plan → watt plan (W = A * V * phases)
        val periods = car.currentPlan.zipWithIndex.map { case (amps, slotIdx) =>
          ChargingSchedulePeriod(
            startPeriod = slotIdx * 900, // 15 min = 900 seconds
            limitWatts = amps * voltage * numPhases,
            numberPhases = Some(numPhases)
          )
        }
        ChargingProfile(
          id = UUID.randomUUID().toString,
          tenantId = tenantId,
          chargingStationId = tx.chargingStationId,
          connectorId = tx.connectorId,
          siteAreaId = Some(siteAreaId),
          siteId = siteId,
          profile = ChargingProfileData(
            chargingProfileId = tx.id.toInt & 0x7fffffff, // positive int from tx id
            transactionId = Some(tx.id),
            stackLevel = 2,
            chargingProfilePurpose = "TxProfile",
            chargingProfileKind = "Absolute",
            recurrencyKind = None,
            validFrom = Some(now),
            validTo = None,
            chargingSchedule = ChargingSchedule(
              startSchedule = Some(scheduleStart),
              durationSecs = Some(periods.size * 900),
              chargingRateUnit = "W",
              periods = if periods.isEmpty then
                List(ChargingSchedulePeriod(0, tx.maxCurrentA * voltage * numPhases))
              else periods,
              minChargingRate = Some(6.0 * voltage * numPhases)
            )
          ),
          createdOn = now,
          lastChangedOn = now
        )
      }
    }

object LiveSmartChargingService:

  val live: ZLayer[
    SmartChargingRepository & SapSmartChargingClient & SmartChargingGatewayClient & SmartChargingConfig,
    Nothing,
    SmartChargingService
  ] =
    ZLayer.fromFunction(new LiveSmartChargingService(_, _, _, _))
