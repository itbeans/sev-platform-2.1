package io.itbeans.ev.asset

import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.kafka.{EvKafkaProducer, Topics}
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._
import zio.http.{Response, Routes}

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// AssetHttpServer — wires Tapir endpoints to ZIO HTTP routes.
// ---------------------------------------------------------------------------

object AssetHttpServer:

  def routes(repo: AssetRepository, kafka: EvKafkaProducer): Routes[Any, Response] =
    val endpoints: List[ZServerEndpoint[Any, ZioStreams & WebSockets]] = List(
      AssetEndpoints.listAssets.zServerLogic { case (tenantId, dynamicOnly, limitQ, skipQ) =>
        val tid = TenantId(tenantId)
        (for
          total <- repo.countAll(tid, dynamicOnly.getOrElse(false))
          items <- repo.findAll(tid, dynamicOnly.getOrElse(false), limitQ.getOrElse(100), skipQ.getOrElse(0))
        yield AssetListResponse(total, items))
          .mapError(_.getMessage)
      },
      AssetEndpoints.createAsset.zServerLogic { case (tenantId, body) =>
        val tid = TenantId(tenantId)
        val now = Instant.now()
        val asset = Asset(
          id = UUID.randomUUID().toString,
          name = body.name,
          companyId = body.companyId,
          siteId = body.siteId,
          siteAreaId = body.siteAreaId,
          assetType = body.assetType,
          fluctuationPercent = body.fluctuationPercent,
          staticValueWatt = body.staticValueWatt,
          coordinates = body.coordinates,
          issuer = body.issuer,
          image = None,
          dynamicAsset = body.dynamicAsset,
          usesPushApi = body.usesPushApi,
          connectionId = body.connectionId,
          meterId = body.meterId,
          excludeFromSmartCharging = body.excludeFromSmartCharging,
          variationThresholdPercent = body.variationThresholdPercent,
          powerWattsLastSmartCharging = None,
          currentConsumption = AssetCurrentConsumption.zero,
          createdOn = now,
          lastChangedOn = now
        )
        repo.upsert(tid, asset).as(asset).mapError(_.getMessage)
      },
      AssetEndpoints.getAsset.zServerLogic { case (id, tenantId) =>
        repo.findById(TenantId(tenantId), id)
          .flatMap {
            case Some(a) => ZIO.succeed(a)
            case None    => ZIO.fail(new Exception(s"Asset $id not found"))
          }
          .mapError(_.getMessage)
      },
      AssetEndpoints.updateAsset.zServerLogic { case (id, tenantId, body) =>
        val tid = TenantId(tenantId)
        (for
          existing <- repo.findById(tid, id).flatMap {
            case Some(a) => ZIO.succeed(a)
            case None    => ZIO.fail(new Exception(s"Asset $id not found"))
          }
          updated = existing.copy(
            name = body.name.getOrElse(existing.name),
            assetType = body.assetType.getOrElse(existing.assetType),
            fluctuationPercent = body.fluctuationPercent.getOrElse(existing.fluctuationPercent),
            staticValueWatt = body.staticValueWatt.getOrElse(existing.staticValueWatt),
            coordinates = body.coordinates.getOrElse(existing.coordinates),
            dynamicAsset = body.dynamicAsset.getOrElse(existing.dynamicAsset),
            usesPushApi = body.usesPushApi.getOrElse(existing.usesPushApi),
            connectionId = body.connectionId.orElse(existing.connectionId),
            meterId = body.meterId.orElse(existing.meterId),
            excludeFromSmartCharging = body.excludeFromSmartCharging.getOrElse(existing.excludeFromSmartCharging),
            variationThresholdPercent = body.variationThresholdPercent.orElse(existing.variationThresholdPercent),
            lastChangedOn = Instant.now()
          )
          _ <- repo.upsert(tid, updated)
        yield updated)
          .mapError(_.getMessage)
      },
      AssetEndpoints.deleteAsset.zServerLogic { case (id, tenantId) =>
        repo.delete(TenantId(tenantId), id).mapError(_.getMessage)
      },

      // Push consumption endpoint: external system pushes real-time data
      AssetEndpoints.pushConsumption.zServerLogic { case (id, tenantId, body) =>
        val tid = TenantId(tenantId)
        (for
          asset <- repo.findById(tid, id).flatMap {
            case Some(a) => ZIO.succeed(a)
            case None    => ZIO.fail(new Exception(s"Asset $id not found"))
          }
          measurement = AssetMeasurement(
            timestamp = body.timestamp,
            instantWatts = body.instantWatts,
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
            consumptionWh = body.consumptionWh,
            stateOfCharge = body.stateOfCharge
          )
          event = AssetConsumptionEvent(
            tenantId = tenantId,
            assetId = asset.id,
            assetName = asset.name,
            siteAreaId = asset.siteAreaId,
            assetType = asset.assetType,
            powerWatts = body.instantWatts,
            measurement = measurement
          )
          _ <- kafka.publish(Topics.assetConsumptions, asset.id, event)
          updated = asset.copy(
            currentConsumption = asset.currentConsumption.copy(
              currentInstantWatts = body.instantWatts,
              currentConsumptionWh = Some(body.consumptionWh),
              currentTotalConsumptionWh = asset.currentConsumption.currentTotalConsumptionWh + body.consumptionWh,
              currentStateOfCharge = body.stateOfCharge.orElse(asset.currentConsumption.currentStateOfCharge),
              lastConsumptionTimestamp = Some(body.timestamp)
            ),
            lastChangedOn = Instant.now()
          )
          _ <- repo.upsert(tid, updated)
        yield ())
          .mapError(_.getMessage)
      }
    )

    ZioHttpInterpreter().toHttp(endpoints)
