package io.itbeans.ev.car

import io.circe._
import io.circe.parser._
import zio._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.MessageDigest
import java.time.{Duration, Instant}
import java.util.Base64

// ---------------------------------------------------------------------------
// EVDatabaseClient — fetches EV vehicle catalogs from ev-database.org API.
//
// GET {url}/{key}  →  JSON array of vehicle objects
//
// Each catalog entry is mapped to a CarCatalog case class.
// Images are fetched separately and stored as base64.
// A SHA-256 content hash enables change detection to avoid redundant upserts.
// ---------------------------------------------------------------------------

trait EVDatabaseClient:
  /** Fetch all catalogs from the remote API */
  def fetchCatalogs: Task[List[CarCatalog]]

  /** Fetch a single image as base64, returning None on HTTP errors */
  def fetchImageAsBase64(url: String): Task[Option[String]]

final class HttpEVDatabaseClient(
    config: EvDatabaseConfig,
    http: HttpClient
) extends EVDatabaseClient:

  override def fetchCatalogs: Task[List[CarCatalog]] =
    ZIO.attemptBlocking {
      val req = HttpRequest
        .newBuilder(URI.create(s"${config.url}/${config.key}"))
        .header("Accept", "application/json")
        .GET()
        .timeout(Duration.ofSeconds(30))
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      resp.body()
    }.flatMap { body =>
      ZIO.fromEither(
        parse(body).flatMap(_.as[List[Json]])
      ).mapError(e => new Exception(s"EVDatabase parse error: $e"))
        .map(_.flatMap(parseEntry))
    }

  override def fetchImageAsBase64(url: String): Task[Option[String]] =
    ZIO.attemptBlocking {
      val req = HttpRequest
        .newBuilder(URI.create(url))
        .GET()
        .timeout(Duration.ofSeconds(15))
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray())
      if resp.statusCode() == 200 then
        Some(Base64.getEncoder.encodeToString(resp.body()))
      else None
    }.catchAll { err =>
      ZIO.logWarning(s"Failed to fetch image $url: ${err.getMessage}").as(None)
    }

  private def parseEntry(j: Json): Option[CarCatalog] =
    val c                  = j.hcursor
    def str(f: String)     = c.downField(f).as[String].toOption
    def int(f: String)     = c.downField(f).as[Int].toOption
    def dbl(f: String)     = c.downField(f).as[Double].toOption
    def strList(f: String) = c.downField(f).as[List[String]].getOrElse(Nil)

    c.downField("id").as[Int].toOption.map { id =>
      val imageUrls = strList("image_urls")
      val now       = Instant.now()
      CarCatalog(
        id = id,
        vehicleMake = str("make").getOrElse(""),
        vehicleModel = str("model").getOrElse(""),
        vehicleModelVersion = str("version"),
        drivetrainPropulsion = str("drive"),
        drivetrainPowerHP = int("power_ps"),
        performanceAcceleration = dbl("acceleration"),
        performanceTopspeed = int("top_speed"),
        rangeReal = int("range_real"),
        rangeWLTP = int("range_wltp"),
        efficiencyReal = dbl("efficiency_real"),
        batteryCapacityUseable = dbl("useable_battery_size"),
        batteryCapacityFull = dbl("battery_capacity"),
        chargePlug = str("charge_standard_plug"),
        chargeStandardPower = dbl("charge_standard_power"),
        chargeStandardPhase = int("charge_standard_phase"),
        chargeStandardPhaseAmp = dbl("charge_standard_phase_amp"),
        chargeStandardChargeTime = int("charge_standard_charge_time"),
        chargeStandardChargeSpeed = dbl("charge_standard_charge_speed"),
        fastChargePlug = str("charge_option_plug"),
        fastChargePowerMax = dbl("charge_option_power_max"),
        miscBody = str("body"),
        miscSegment = str("segment"),
        miscSeats = int("seats"),
        imageUrls = imageUrls,
        image = None, // fetched separately
        hash = None,  // computed after full fetch
        imagesHash = Some(sha256(imageUrls.mkString(","))),
        createdOn = now,
        lastChangedOn = now
      )
    }

object HttpEVDatabaseClient:

  val live: ZLayer[CarConfig, Nothing, EVDatabaseClient] =
    ZLayer.fromZIO {
      ZIO.service[CarConfig].map { cfg =>
        val http = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(10))
          .build()
        new HttpEVDatabaseClient(cfg.evDatabase, http)
      }
    }

// ---------------------------------------------------------------------------
// SynchronizeCarsTask — periodic ZIO Schedule task that syncs the catalog.
//
// Algorithm (mirrors TypeScript synchronizeCarCatalogs):
//   1. Fetch all catalogs from EVDatabase.
//   2. For each, compute a SHA-256 content hash.
//   3. If hash differs from stored hash → upsert.
//   4. If imageUrls changed → re-fetch first image as thumbnail, upsert.
//   5. Return SyncResult(inSuccess, inError).
// ---------------------------------------------------------------------------

final class SynchronizeCarsTask(
    client: EVDatabaseClient,
    catalog: CarCatalogRepository
):

  def run: Task[SyncResult] =
    for
      _        <- ZIO.logInfo("SynchronizeCarsTask: starting catalog sync")
      catalogs <- client.fetchCatalogs
      results  <- ZIO.foreachPar(catalogs)(syncOne).withParallelism(4)
      success = results.count(identity)
      errors  = results.count(!_)
      _ <- ZIO.logInfo(s"SynchronizeCarsTask: done — $success ok, $errors errors")
    yield SyncResult(success, errors)

  private def syncOne(incoming: CarCatalog): Task[Boolean] =
    (for
      existing <- catalog.findById(incoming.id)
      newHash       = sha256(computeHashInput(incoming))
      imagesChanged = existing.forall(e => e.imagesHash != incoming.imagesHash)
      needsUpdate   = existing.forall(e => e.hash != Some(newHash))
      thumb <-
        if imagesChanged && incoming.imageUrls.nonEmpty
        then client.fetchImageAsBase64(incoming.imageUrls.head)
        else ZIO.succeed(existing.flatMap(_.image))
      updated = incoming.copy(
        hash = Some(newHash),
        image = thumb,
        imagesHash = incoming.imagesHash,
        createdOn = existing.map(_.createdOn).getOrElse(incoming.createdOn),
        lastChangedOn = Instant.now()
      )
      _ <-
        if needsUpdate || imagesChanged
        then
          catalog.upsert(updated) *> ZIO.logDebug(
            s"Upserted catalog ${incoming.id} ${incoming.vehicleMake} ${incoming.vehicleModel}"
          )
        else ZIO.logDebug(s"Catalog ${incoming.id} unchanged — skipping")
    yield true)
      .catchAll { err =>
        ZIO.logError(s"Failed to sync catalog id=${incoming.id}: ${err.getMessage}").as(false)
      }

object SynchronizeCarsTask:

  val live: ZLayer[EVDatabaseClient & CarCatalogRepository, Nothing, SynchronizeCarsTask] =
    ZLayer.fromFunction(new SynchronizeCarsTask(_, _))

// Shared hash utility
private[car] def sha256(input: String): String =
  val digest = MessageDigest.getInstance("SHA-256")
  val bytes  = digest.digest(input.getBytes("UTF-8"))
  bytes.map("%02x".format(_)).mkString

private def computeHashInput(c: CarCatalog): String =
  s"${c.id}|${c.vehicleMake}|${c.vehicleModel}|${c.vehicleModelVersion}|" +
    s"${c.batteryCapacityUseable}|${c.rangeReal}|${c.chargeStandardPower}|${c.fastChargePowerMax}"
