package io.itbeans.ev.smartcharging

import io.circe.parser.decode
import io.circe.syntax._
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.FindOneAndReplaceOptions
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// SmartChargingRepository — MongoDB access for the smart-charging service.
//
// Collection layout (collection-level multi-tenancy):
//   {tenantId}.chargingprofiles  — OCPP ChargingProfile documents (owned here)
//   {tenantId}.siteareas         — site area read model (read-only)
//   {tenantId}.transactions      — active transaction subset (read-only)
//
// Uses the reactive MongoDB Scala driver; futures are lifted into ZIO effects.
// ---------------------------------------------------------------------------

trait SmartChargingRepository:
  def saveProfile(profile: ChargingProfile): Task[Unit]
  def findProfiles(tenantId: String, siteAreaId: String): Task[List[ChargingProfile]]
  def deleteProfilesForStation(tenantId: String, chargingStationId: String): Task[Unit]
  def findSiteArea(tenantId: String, siteAreaId: String): Task[Option[SiteAreaSC]]
  def findActiveTransactions(tenantId: String, siteAreaId: String): Task[List[ActiveTransactionSC]]

// ── MongoDB implementation ────────────────────────────────────────────────

final class MongoSmartChargingRepository(db: MongoDatabase) extends SmartChargingRepository:

  // Collection helpers — use the {tenantId}.{collectionName} naming convention
  private def profilesCol(tenantId: String)     = db.getCollection[Document](s"$tenantId.chargingprofiles")
  private def siteAreasCol(tenantId: String)    = db.getCollection[Document](s"$tenantId.siteareas")
  private def transactionsCol(tenantId: String) = db.getCollection[Document](s"$tenantId.transactions")

  // ── Charging profiles ─────────────────────────────────────────────────

  def saveProfile(profile: ChargingProfile): Task[Unit] =
    ZIO.fromFuture { _ =>
      val doc = Document.parse(profile.asJson.noSpaces)
      val filter = and(
        equal("chargingStationId", profile.chargingStationId),
        equal[Integer]("connectorId", profile.connectorId),
        equal[Integer]("profile.chargingProfileId", profile.profile.chargingProfileId)
      )
      val opts = FindOneAndReplaceOptions().upsert(true)
      profilesCol(profile.tenantId).findOneAndReplace(filter, doc, opts).toFuture()
    }.unit

  def findProfiles(tenantId: String, siteAreaId: String): Task[List[ChargingProfile]] =
    ZIO.fromFuture { _ =>
      profilesCol(tenantId)
        .find(equal("siteAreaId", siteAreaId))
        .toFuture()
    }.map { docs =>
      docs.flatMap { d =>
        decode[ChargingProfile](d.toJson()) match
          case Right(p)  => Some(p)
          case Left(err) =>
            // Skip malformed profiles — don't crash the optimizer run
            println(s"[SmartCharging] Failed to decode ChargingProfile: $err")
            None
      }.toList
    }

  def deleteProfilesForStation(tenantId: String, chargingStationId: String): Task[Unit] =
    ZIO.fromFuture { _ =>
      profilesCol(tenantId)
        .deleteMany(equal("chargingStationId", chargingStationId))
        .toFuture()
    }.unit

  // ── Site area ─────────────────────────────────────────────────────────

  def findSiteArea(tenantId: String, siteAreaId: String): Task[Option[SiteAreaSC]] =
    ZIO.fromFuture { _ =>
      siteAreasCol(tenantId)
        .find(equal("_id", siteAreaId))
        .first()
        .toFutureOption()
    }.map(_.flatMap(d => decode[SiteAreaSC](d.toJson()).toOption))

  // ── Active transactions ────────────────────────────────────────────────

  def findActiveTransactions(tenantId: String, siteAreaId: String): Task[List[ActiveTransactionSC]] =
    ZIO.fromFuture { _ =>
      transactionsCol(tenantId)
        .find(
          and(
            equal("siteAreaID", siteAreaId),
            not(exists("stop"))
          )
        )
        .toFuture()
    }.map(_.map(docToActiveTx).toList)

  private def docToActiveTx(d: Document): ActiveTransactionSC =
    ActiveTransactionSC(
      id = Option(d.getLong("id")).map(_.longValue).getOrElse(0L),
      chargingStationId = Option(d.getString("chargeBoxID")).getOrElse(""),
      connectorId = Option(d.getInteger("connectorId")).map(_.intValue).getOrElse(1),
      stateOfCharge = Option(d.getInteger("stateOfCharge")).map(_.intValue),
      departureTime = Option(d.getLong("departureTime"))
        .map(l => Instant.ofEpochMilli(l.longValue)),
      targetSoC = Option(d.getInteger("targetStateOfCharge")).map(_.intValue),
      maxCurrentA = Option(d.getDouble("maxCurrentA")).map(_.doubleValue).getOrElse(32.0),
      numberOfPhases = Option(d.getInteger("numberOfPhases")).map(_.intValue).getOrElse(3)
    )

object MongoSmartChargingRepository:

  val live: ZLayer[MongoDatabase, Nothing, SmartChargingRepository] =
    ZLayer.fromFunction(new MongoSmartChargingRepository(_))
