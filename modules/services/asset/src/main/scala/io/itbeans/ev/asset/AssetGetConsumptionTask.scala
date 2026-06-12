package io.itbeans.ev.asset

import io.itbeans.ev.asset.connector.AssetConnectorRegistry
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.kafka.{EvKafkaProducer, Topics}
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// AssetGetConsumptionTask — polls all dynamic assets for current consumption.
//
// Mirrors TypeScript AssetGetConsumptionTask flow:
//   1. For each tenant (in this impl: passed as a parameter)
//   2. Load all assets with dynamicAsset=true and usesPushApi=false
//   3. For each asset with a connectionId and meterId:
//      a. Load vendor connection config
//      b. Find the appropriate connector
//      c. Call connector.fetchConsumptions(connection, meterId, assetType, since, false)
//   4. For each measurement returned:
//      a. Update asset's currentConsumption fields
//      b. Save asset back to MongoDB
//      c. Publish AssetConsumptionEvent to Kafka topic asset.consumptions
//   5. Errors per asset are logged and do not stop processing other assets.
//
// Minimum dedup gap: 50 seconds between consecutive consumption records.
// Smart charging integration: ev-smart-charging consumes asset.consumptions
// and translates each event into a debounced "Reoptimize" trigger.
// ---------------------------------------------------------------------------

final class AssetGetConsumptionTask(
    assetRepo: AssetRepository,
    connectionRepo: AssetConnectionRepository,
    connectors: AssetConnectorRegistry,
    kafka: EvKafkaProducer
):

  private val MinGapSeconds = 50L

  /** Run one full poll cycle for all dynamic assets of a tenant. */
  def run(tenantId: TenantId): Task[Unit] =
    for
      assets <- assetRepo.findAll(tenantId, dynamicOnly = true)
      _      <- ZIO.logInfo(s"AssetGetConsumptionTask: polling ${assets.length} assets for tenant=${tenantId.value}")
      _ <- ZIO.foreachDiscard(assets)(a =>
        pollAsset(tenantId, a).catchAll { err =>
          ZIO.logError(s"Asset ${a.id} poll failed: ${err.getMessage}")
        }
      )
    yield ()

  private def pollAsset(tenantId: TenantId, asset: Asset): Task[Unit] =
    (asset.connectionId, asset.meterId) match
      case (None, _) | (_, None) =>
        ZIO.logDebug(s"Asset ${asset.id} has no connectionId/meterId — skipping")
      case (Some(connectionId), Some(meterId)) =>
        for
          connOpt <- connectionRepo.findById(tenantId, connectionId)
          conn <- connOpt match
            case None    => ZIO.fail(new Exception(s"Connection $connectionId not found"))
            case Some(c) => ZIO.succeed(c)
          _ <- connectors.get(conn.connectorType) match
            case None    => ZIO.fail(new Exception(s"No connector for type ${conn.connectorType}"))
            case Some(c) => fetchAndRecord(tenantId, asset, conn, meterId, c)
        yield ()

  private def fetchAndRecord(
      tenantId: TenantId,
      asset: Asset,
      conn: AssetConnection,
      meterId: String,
      connector: io.itbeans.ev.asset.connector.AssetConnector
  ): Task[Unit] =
    for
      measurements <- connector.fetchConsumptions(
        conn,
        meterId,
        asset.assetType,
        since = asset.currentConsumption.lastConsumptionTimestamp,
        manualCall = false
      )
      // Filter out measurements that are too close to the last recorded one
      filtered = dedup(measurements, asset.currentConsumption.lastConsumptionTimestamp)
      _ <- ZIO.when(filtered.isEmpty)(
        ZIO.logDebug(s"Asset ${asset.id}: no new measurements (dedup filtered all)")
      )
      _ <- ZIO.foldLeft(filtered)(asset)((current, m) => recordMeasurement(tenantId, current, m))
    yield ()

  private def dedup(measurements: List[AssetMeasurement], lastTs: Option[Instant]): List[AssetMeasurement] =
    lastTs match
      case None    => measurements
      case Some(t) => measurements.filter(m => m.timestamp.getEpochSecond - t.getEpochSecond >= MinGapSeconds)

  private def recordMeasurement(tenantId: TenantId, asset: Asset, m: AssetMeasurement): Task[Asset] =
    val newConsumption = asset.currentConsumption.copy(
      currentInstantWatts = m.instantWatts,
      currentInstantWattsL1 = m.instantWattsL1.orElse(asset.currentConsumption.currentInstantWattsL1),
      currentInstantWattsL2 = m.instantWattsL2.orElse(asset.currentConsumption.currentInstantWattsL2),
      currentInstantWattsL3 = m.instantWattsL3.orElse(asset.currentConsumption.currentInstantWattsL3),
      currentInstantAmps = m.instantAmps.orElse(asset.currentConsumption.currentInstantAmps),
      currentInstantAmpsL1 = m.instantAmpsL1.orElse(asset.currentConsumption.currentInstantAmpsL1),
      currentInstantAmpsL2 = m.instantAmpsL2.orElse(asset.currentConsumption.currentInstantAmpsL2),
      currentInstantAmpsL3 = m.instantAmpsL3.orElse(asset.currentConsumption.currentInstantAmpsL3),
      currentInstantVolts = m.instantVolts.orElse(asset.currentConsumption.currentInstantVolts),
      currentInstantVoltsL1 = m.instantVoltsL1.orElse(asset.currentConsumption.currentInstantVoltsL1),
      currentInstantVoltsL2 = m.instantVoltsL2.orElse(asset.currentConsumption.currentInstantVoltsL2),
      currentInstantVoltsL3 = m.instantVoltsL3.orElse(asset.currentConsumption.currentInstantVoltsL3),
      currentConsumptionWh = Some(m.consumptionWh),
      currentTotalConsumptionWh = asset.currentConsumption.currentTotalConsumptionWh + m.consumptionWh,
      currentStateOfCharge = m.stateOfCharge.orElse(asset.currentConsumption.currentStateOfCharge),
      lastConsumptionTimestamp = Some(m.timestamp)
    )
    val updatedAsset = asset.copy(
      currentConsumption = newConsumption,
      lastChangedOn = Instant.now()
    )
    val event = AssetConsumptionEvent(
      tenantId = tenantId.value,
      assetId = asset.id,
      assetName = asset.name,
      siteAreaId = asset.siteAreaId,
      assetType = asset.assetType,
      powerWatts = m.instantWatts,
      measurement = m
    )
    for
      _ <- assetRepo.upsert(tenantId, updatedAsset)
      _ <- kafka.publish(Topics.assetConsumptions, asset.id, event)
        .catchAll(err => ZIO.logWarning(s"Kafka publish failed for asset ${asset.id}: ${err.getMessage}"))
      _ <- ZIO.logDebug(
        s"Asset ${asset.id} (${asset.name}): ${m.instantWatts}W @ ${m.timestamp}"
      )
    yield updatedAsset

object AssetGetConsumptionTask:

  val live: ZLayer[
    AssetRepository & AssetConnectionRepository & AssetConnectorRegistry & EvKafkaProducer,
    Nothing,
    AssetGetConsumptionTask
  ] =
    ZLayer.fromFunction(new AssetGetConsumptionTask(_, _, _, _))
