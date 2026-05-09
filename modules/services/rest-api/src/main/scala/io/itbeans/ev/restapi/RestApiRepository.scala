package io.itbeans.ev.restapi

import io.circe.parser.decode
import io.circe.generic.auto._
import RestApiCodecs.given
import io.itbeans.ev.domain._
import io.itbeans.ev.domain.Tag // explicit import resolves ambiguity with zio.Tag
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import zio._

import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// RestApiRepository — unified MongoDB access for ev-rest-api.
//
// Multi-tenancy: each collection is accessed as {tenantId}.{resource}
// following the ev-server convention (DatabaseUtils.ts → getCollection).
//
// All write paths accept raw Document (caller serialises via Circe) so that
// the repository remains free of HTTP-layer types.
// ---------------------------------------------------------------------------

/** Pagination envelope returned by all list operations. */
case class PagedResult[T](result: List[T], count: Int)

// ── Repository trait ──────────────────────────────────────────────────────

trait RestApiRepository:

  // ChargingStations
  def listStations(
      tenantId: String,
      limit: Int,
      skip: Int,
      siteId: Option[String],
      siteAreaId: Option[String]
  ): Task[PagedResult[ChargingStation]]
  def getStation(tenantId: String, id: String): Task[Option[ChargingStation]]
  def createStation(tenantId: String, doc: Document): Task[Unit]
  def updateStation(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteStation(tenantId: String, id: String): Task[Boolean]

  // Sites
  def listSites(tenantId: String, limit: Int, skip: Int, search: Option[String]): Task[PagedResult[Site]]
  def getSite(tenantId: String, id: String): Task[Option[Site]]
  def createSite(tenantId: String, doc: Document): Task[Unit]
  def updateSite(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteSite(tenantId: String, id: String): Task[Boolean]

  // SiteAreas
  def listSiteAreas(tenantId: String, limit: Int, skip: Int, siteId: Option[String]): Task[PagedResult[SiteArea]]
  def getSiteArea(tenantId: String, id: String): Task[Option[SiteArea]]
  def createSiteArea(tenantId: String, doc: Document): Task[Unit]
  def updateSiteArea(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteSiteArea(tenantId: String, id: String): Task[Boolean]

  // Companies
  def listCompanies(tenantId: String, limit: Int, skip: Int): Task[PagedResult[Company]]
  def getCompany(tenantId: String, id: String): Task[Option[Company]]
  def createCompany(tenantId: String, doc: Document): Task[Unit]
  def updateCompany(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteCompany(tenantId: String, id: String): Task[Boolean]

  // Transactions
  def listTransactions(
      tenantId: String,
      limit: Int,
      skip: Int,
      inProgress: Option[Boolean],
      stationId: Option[String],
      userId: Option[String]
  ): Task[PagedResult[Transaction]]
  def getTransaction(tenantId: String, id: Long): Task[Option[Transaction]]
  def deleteTransaction(tenantId: String, id: Long): Task[Boolean]

  // Users
  def listUsers(
      tenantId: String,
      limit: Int,
      skip: Int,
      role: Option[String],
      search: Option[String]
  ): Task[PagedResult[User]]
  def getUser(tenantId: String, id: String): Task[Option[User]]
  def createUser(tenantId: String, doc: Document): Task[Unit]
  def updateUser(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteUser(tenantId: String, id: String): Task[Boolean]

  // Tags
  def listTags(
      tenantId: String,
      limit: Int,
      skip: Int,
      userId: Option[String],
      active: Option[Boolean]
  ): Task[PagedResult[Tag]]
  def getTag(tenantId: String, id: String): Task[Option[Tag]]
  def createTag(tenantId: String, doc: Document): Task[Unit]
  def updateTag(tenantId: String, id: String, doc: Document): Task[Boolean]
  def deleteTag(tenantId: String, id: String): Task[Boolean]

  // Settings (per-component JSON blobs: Pricing, Billing, SmartCharging, etc.)
  def getSetting(tenantId: String, identifier: String): Task[Option[io.circe.Json]]
  def upsertSetting(tenantId: String, identifier: String, body: io.circe.Json): Task[Unit]

  // Registration tokens (OCPP station onboarding)
  def listRegistrationTokens(tenantId: String, limit: Int, skip: Int): Task[PagedResult[io.circe.Json]]
  def getRegistrationToken(tenantId: String, id: String): Task[Option[io.circe.Json]]
  def createRegistrationToken(tenantId: String, doc: Document): Task[Unit]
  def deleteRegistrationToken(tenantId: String, id: String): Task[Boolean]

// ── MongoDB implementation ───────────────────────────────────────────────

final class MongoRestApiRepository(db: MongoDatabase) extends RestApiRepository:

  // Each collection is accessed as "{tenantId}.{collectionName}"
  private def col(tenantId: String, name: String) =
    db.getCollection[Document](s"$tenantId.$name")

  // ── ChargingStations ─────────────────────────────────────────────────────

  def listStations(
      tenantId: String,
      limit: Int,
      skip: Int,
      siteId: Option[String],
      siteAreaId: Option[String]
  ): Task[PagedResult[ChargingStation]] =
    val filters = List(
      siteId.map(id => equal("siteID", id)),
      siteAreaId.map(id => equal("siteAreaID", id))
    ).flatten
    val filter = if filters.isEmpty then new Document() else and(filters*)
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .find(filter)
        .sort(descending("lastChangedOn"))
        .skip(skip)
        .limit(limit)
        .toFuture()
    }.map { docs =>
      PagedResult(docs.flatMap(docToStation).toList, docs.size)
    }

  def getStation(tenantId: String, id: String): Task[Option[ChargingStation]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToStation))

  def createStation(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations").insertOne(doc).toFuture()
    }.unit

  def updateStation(tenantId: String, id: String, doc: Document): Task[Boolean] =
    import org.mongodb.scala.model.Updates.set
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .updateOne(equal("_id", id), new Document("$set", doc))
        .toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteStation(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Sites ─────────────────────────────────────────────────────────────────

  def listSites(tenantId: String, limit: Int, skip: Int, search: Option[String]): Task[PagedResult[Site]] =
    val filter = search
      .map(s => regex("name", s, "i"))
      .getOrElse(new Document())
    ZIO.fromFuture { _ =>
      col(tenantId, "sites").find(filter).skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToSite).toList, docs.size))

  def getSite(tenantId: String, id: String): Task[Option[Site]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "sites").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToSite))

  def createSite(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "sites").insertOne(doc).toFuture()).unit

  def updateSite(tenantId: String, id: String, doc: Document): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "sites")
        .updateOne(equal("_id", id), new Document("$set", doc)).toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteSite(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "sites").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── SiteAreas ─────────────────────────────────────────────────────────────

  def listSiteAreas(tenantId: String, limit: Int, skip: Int, siteId: Option[String]): Task[PagedResult[SiteArea]] =
    val filter = siteId.map(id => equal("siteID", id)).getOrElse(new Document())
    ZIO.fromFuture { _ =>
      col(tenantId, "siteareas").find(filter).skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToSiteArea).toList, docs.size))

  def getSiteArea(tenantId: String, id: String): Task[Option[SiteArea]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "siteareas").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToSiteArea))

  def createSiteArea(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "siteareas").insertOne(doc).toFuture()).unit

  def updateSiteArea(tenantId: String, id: String, doc: Document): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "siteareas")
        .updateOne(equal("_id", id), new Document("$set", doc)).toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteSiteArea(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "siteareas").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Companies ─────────────────────────────────────────────────────────────

  def listCompanies(tenantId: String, limit: Int, skip: Int): Task[PagedResult[Company]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "companies").find().skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToCompany).toList, docs.size))

  def getCompany(tenantId: String, id: String): Task[Option[Company]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "companies").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToCompany))

  def createCompany(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "companies").insertOne(doc).toFuture()).unit

  def updateCompany(tenantId: String, id: String, doc: Document): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "companies")
        .updateOne(equal("_id", id), new Document("$set", doc)).toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteCompany(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "companies").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Transactions ──────────────────────────────────────────────────────────

  def listTransactions(
      tenantId: String,
      limit: Int,
      skip: Int,
      inProgress: Option[Boolean],
      stationId: Option[String],
      userId: Option[String]
  ): Task[PagedResult[Transaction]] =
    val filters = List(
      inProgress.map(v => equal("stop", !v)),
      stationId.map(id => equal("chargeBoxID", id)),
      userId.map(id => equal("userID", id))
    ).flatten
    val filter = if filters.isEmpty then new Document() else and(filters*)
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions")
        .find(filter)
        .sort(descending("timestamp"))
        .skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToTransaction).toList, docs.size))

  def getTransaction(tenantId: String, id: Long): Task[Option[Transaction]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToTransaction))

  def deleteTransaction(tenantId: String, id: Long): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Users ─────────────────────────────────────────────────────────────────

  def listUsers(
      tenantId: String,
      limit: Int,
      skip: Int,
      role: Option[String],
      search: Option[String]
  ): Task[PagedResult[User]] =
    val filters = List(
      role.map(r => equal("role", r)),
      search.map(s => regex("email", s, "i"))
    ).flatten
    val filter = if filters.isEmpty then new Document() else and(filters*)
    ZIO.fromFuture { _ =>
      col(tenantId, "users").find(filter).skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToUser).toList, docs.size))

  def getUser(tenantId: String, id: String): Task[Option[User]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "users").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToUser))

  def createUser(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "users").insertOne(doc).toFuture()).unit

  def updateUser(tenantId: String, id: String, doc: Document): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "users")
        .updateOne(equal("_id", id), new Document("$set", doc)).toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteUser(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "users").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Tags ──────────────────────────────────────────────────────────────────

  def listTags(
      tenantId: String,
      limit: Int,
      skip: Int,
      userId: Option[String],
      active: Option[Boolean]
  ): Task[PagedResult[Tag]] =
    val filters = List(
      userId.map(id => equal("userID", id)),
      active.map(v => equal("active", v))
    ).flatten
    val filter = if filters.isEmpty then new Document() else and(filters*)
    ZIO.fromFuture { _ =>
      col(tenantId, "tags").find(filter).skip(skip).limit(limit).toFuture()
    }.map(docs => PagedResult(docs.flatMap(docToTag).toList, docs.size))

  def getTag(tenantId: String, id: String): Task[Option[Tag]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "tags").find(equal("_id", id)).headOption()
    }.map(_.flatMap(docToTag))

  def createTag(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "tags").insertOne(doc).toFuture()).unit

  def updateTag(tenantId: String, id: String, doc: Document): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "tags")
        .updateOne(equal("_id", id), new Document("$set", doc)).toFuture()
    }.map(_.getModifiedCount > 0)

  def deleteTag(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "tags").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── Settings ──────────────────────────────────────────────────────────────

  def getSetting(tenantId: String, identifier: String): Task[Option[io.circe.Json]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "settings").find(equal("identifier", identifier)).headOption()
    }.map(_.flatMap(d => io.circe.parser.parse(d.toJson).toOption))

  def upsertSetting(tenantId: String, identifier: String, body: io.circe.Json): Task[Unit] =
    import org.mongodb.scala.model.ReplaceOptions
    val doc = org.bson.Document.parse(body.noSpaces).append("identifier", identifier)
    ZIO.fromFuture { _ =>
      col(tenantId, "settings")
        .replaceOne(equal("identifier", identifier), doc, ReplaceOptions().upsert(true))
        .toFuture()
    }.unit

  // ── Registration tokens ────────────────────────────────────────────────────

  def listRegistrationTokens(tenantId: String, limit: Int, skip: Int): Task[PagedResult[io.circe.Json]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "registrationtokens").find().skip(skip).limit(limit).toFuture()
    }.map { docs =>
      val jsons = docs.flatMap(d => io.circe.parser.parse(d.toJson).toOption).toList
      PagedResult(jsons, jsons.size)
    }

  def getRegistrationToken(tenantId: String, id: String): Task[Option[io.circe.Json]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "registrationtokens").find(equal("_id", id)).headOption()
    }.map(_.flatMap(d => io.circe.parser.parse(d.toJson).toOption))

  def createRegistrationToken(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture(_ => col(tenantId, "registrationtokens").insertOne(doc).toFuture()).unit

  def deleteRegistrationToken(tenantId: String, id: String): Task[Boolean] =
    ZIO.fromFuture { _ =>
      col(tenantId, "registrationtokens").deleteOne(equal("_id", id)).toFuture()
    }.map(_.getDeletedCount > 0)

  // ── BSON → domain mappers (simplified — delegates to Circe JSON round-trip) ──

  private def docToStation(d: Document): Option[ChargingStation] =
    decode[ChargingStation](d.toJson).toOption

  private def docToSite(d: Document): Option[Site] =
    decode[Site](d.toJson).toOption

  private def docToSiteArea(d: Document): Option[SiteArea] =
    decode[SiteArea](d.toJson).toOption

  private def docToCompany(d: Document): Option[Company] =
    decode[Company](d.toJson).toOption

  private def docToTransaction(d: Document): Option[Transaction] =
    decode[Transaction](d.toJson).toOption

  private def docToUser(d: Document): Option[User] =
    decode[User](d.toJson).toOption

  private def docToTag(d: Document): Option[Tag] =
    decode[Tag](d.toJson).toOption

object RestApiRepository:

  val live: ZLayer[MongoDatabase, Nothing, RestApiRepository] =
    ZLayer.fromFunction(new MongoRestApiRepository(_))
