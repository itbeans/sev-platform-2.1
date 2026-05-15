package io.itbeans.ev.roaming

import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.mongo.TenantCollection
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, Sorts}
import zio._

import java.time.Instant
import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// RoamingRepository — MongoDB access for OcpiEndpoint, OicpEndpoint,
// and read-only access to the transactions collection for CDR generation.
//
// Collections:
//   {tenantId}.ocpiendpoints  — OCPI partner registrations
//   {tenantId}.oicpendpoints  — OICP partner registrations
//   {tenantId}.transactions   — read-only (owned by ev-ocpp-processor)
// ---------------------------------------------------------------------------

trait OcpiEndpointRepository:
  def findAll(tenantId: TenantId): Task[List[OcpiEndpoint]]
  def findById(tenantId: TenantId, id: String): Task[Option[OcpiEndpoint]]
  def findByLocalToken(tenantId: TenantId, token: String): Task[Option[OcpiEndpoint]]
  def findRegistered(tenantId: TenantId): Task[List[OcpiEndpoint]]
  def upsert(tenantId: TenantId, ep: OcpiEndpoint): Task[Unit]
  def delete(tenantId: TenantId, id: String): Task[Unit]

trait OicpEndpointRepository:
  def findAll(tenantId: TenantId): Task[List[OicpEndpoint]]
  def findById(tenantId: TenantId, id: String): Task[Option[OicpEndpoint]]
  def findRegistered(tenantId: TenantId): Task[List[OicpEndpoint]]
  def upsert(tenantId: TenantId, ep: OicpEndpoint): Task[Unit]
  def delete(tenantId: TenantId, id: String): Task[Unit]

trait TransactionReadRepository:
  def findById(tenantId: TenantId, transactionId: Long): Task[Option[RoamingTransaction]]

trait ChargingStationLocationRepository:
  def findLocation(tenantId: TenantId, stationId: String): Task[Option[StationLocation]]

// ── MongoOcpiEndpointRepository ───────────────────────────────────────────

final class MongoOcpiEndpointRepository(db: MongoDatabase) extends OcpiEndpointRepository:

  private def col(tid: TenantId) = TenantCollection[Document]("ocpiendpoints", tid, db)

  override def findAll(tid: TenantId): Task[List[OcpiEndpoint]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.ocpiendpoints").find().toFuture()
    ).map(_.map(docToEndpoint).toList)

  override def findById(tid: TenantId, id: String): Task[Option[OcpiEndpoint]] =
    col(tid).findOne(equal("_id", id)).map(_.map(docToEndpoint))

  override def findByLocalToken(tid: TenantId, token: String): Task[Option[OcpiEndpoint]] =
    col(tid).findOne(equal("localToken", token)).map(_.map(docToEndpoint))

  override def findRegistered(tid: TenantId): Task[List[OcpiEndpoint]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.ocpiendpoints")
        .find(equal("status", OcpiEndpointStatus.Registered.code))
        .toFuture()
    ).map(_.map(docToEndpoint).toList)

  override def upsert(tid: TenantId, ep: OcpiEndpoint): Task[Unit] =
    ZIO.fromFuture { _ =>
      val opts = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tid.value}.ocpiendpoints")
        .findOneAndReplace(equal("_id", ep.id), endpointToDoc(ep), opts)
        .toFuture()
    }.unit

  override def delete(tid: TenantId, id: String): Task[Unit] =
    col(tid).deleteOne(equal("_id", id))

  private def endpointToDoc(ep: OcpiEndpoint): Document =
    val d = new Document("_id", ep.id)
      .append("tenantId", ep.tenantId)
      .append("countryCode", ep.countryCode)
      .append("partyId", ep.partyId)
      .append("role", ep.role.code)
      .append("name", ep.name)
      .append("baseUrl", ep.baseUrl)
      .append("token", ep.token)
      .append("localToken", ep.localToken)
      .append("status", ep.status.code)
      .append("backgroundPatchJob", ep.backgroundPatchJob)
      .append("createdOn", ep.createdOn.toEpochMilli)
      .append("lastChangedOn", ep.lastChangedOn.toEpochMilli)
    ep.locationsUrl.foreach(d.append("locationsUrl", _))
    ep.sessionsUrl.foreach(d.append("sessionsUrl", _))
    ep.cdrsUrl.foreach(d.append("cdrsUrl", _))
    ep.tariffUrl.foreach(d.append("tariffUrl", _))
    ep.tokensUrl.foreach(d.append("tokensUrl", _))
    ep.commandsUrl.foreach(d.append("commandsUrl", _))
    ep.lastPatchJobOn.foreach(t => d.append("lastPatchJobOn", t.toEpochMilli))
    ep.lastPatchJobResult.foreach(d.append("lastPatchJobResult", _))
    d

  private def docToEndpoint(d: Document): OcpiEndpoint =
    OcpiEndpoint(
      id = d.getString("_id"),
      tenantId = d.getString("tenantId"),
      countryCode = d.getString("countryCode"),
      partyId = d.getString("partyId"),
      role = OcpiRole.fromCode(d.getString("role")).getOrElse(OcpiRole.CPO),
      name = Option(d.getString("name")).getOrElse(""),
      baseUrl = d.getString("baseUrl"),
      token = d.getString("token"),
      localToken = d.getString("localToken"),
      status = OcpiEndpointStatus.fromCode(d.getString("status")).getOrElse(OcpiEndpointStatus.Unregistered),
      backgroundPatchJob = Option(d.getBoolean("backgroundPatchJob")).exists(_.booleanValue),
      locationsUrl = Option(d.getString("locationsUrl")),
      sessionsUrl = Option(d.getString("sessionsUrl")),
      cdrsUrl = Option(d.getString("cdrsUrl")),
      tariffUrl = Option(d.getString("tariffUrl")),
      tokensUrl = Option(d.getString("tokensUrl")),
      commandsUrl = Option(d.getString("commandsUrl")),
      lastPatchJobOn = Option(d.getLong("lastPatchJobOn")).map(l => Instant.ofEpochMilli(l.longValue)),
      lastPatchJobResult = Option(d.getString("lastPatchJobResult")),
      createdOn = Instant.ofEpochMilli(Option(d.getLong("createdOn")).map(_.longValue).getOrElse(0L)),
      lastChangedOn = Instant.ofEpochMilli(Option(d.getLong("lastChangedOn")).map(_.longValue).getOrElse(0L))
    )

object MongoOcpiEndpointRepository:

  val live: ZLayer[MongoDatabase, Nothing, OcpiEndpointRepository] =
    ZLayer.fromFunction(new MongoOcpiEndpointRepository(_))

// ── MongoOicpEndpointRepository ───────────────────────────────────────────

final class MongoOicpEndpointRepository(db: MongoDatabase) extends OicpEndpointRepository:

  private def col(tid: TenantId) = TenantCollection[Document]("oicpendpoints", tid, db)

  override def findAll(tid: TenantId): Task[List[OicpEndpoint]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.oicpendpoints").find().toFuture()
    ).map(_.map(docToEndpoint).toList)

  override def findById(tid: TenantId, id: String): Task[Option[OicpEndpoint]] =
    col(tid).findOne(equal("_id", id)).map(_.map(docToEndpoint))

  override def findRegistered(tid: TenantId): Task[List[OicpEndpoint]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.oicpendpoints")
        .find(equal("status", OicpEndpointStatus.Registered.code))
        .toFuture()
    ).map(_.map(docToEndpoint).toList)

  override def upsert(tid: TenantId, ep: OicpEndpoint): Task[Unit] =
    ZIO.fromFuture { _ =>
      val opts = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tid.value}.oicpendpoints")
        .findOneAndReplace(equal("_id", ep.id), endpointToDoc(ep), opts)
        .toFuture()
    }.unit

  override def delete(tid: TenantId, id: String): Task[Unit] =
    col(tid).deleteOne(equal("_id", id))

  private def endpointToDoc(ep: OicpEndpoint): Document =
    val d = new Document("_id", ep.id)
      .append("tenantId", ep.tenantId)
      .append("name", ep.name)
      .append("baseUrl", ep.baseUrl)
      .append("countryCode", ep.countryCode)
      .append("partyId", ep.partyId)
      .append("status", ep.status.code)
      .append("operatorId", ep.operatorId)
      .append("operatorName", ep.operatorName)
      .append("createdOn", ep.createdOn.toEpochMilli)
      .append("lastChangedOn", ep.lastChangedOn.toEpochMilli)
    ep.lastPatchJobOn.foreach(t => d.append("lastPatchJobOn", t.toEpochMilli))
    ep.lastPatchJobResult.foreach(d.append("lastPatchJobResult", _))
    d

  private def docToEndpoint(d: Document): OicpEndpoint =
    OicpEndpoint(
      id = d.getString("_id"),
      tenantId = d.getString("tenantId"),
      name = Option(d.getString("name")).getOrElse(""),
      baseUrl = d.getString("baseUrl"),
      countryCode = d.getString("countryCode"),
      partyId = d.getString("partyId"),
      status = OicpEndpointStatus.fromCode(d.getString("status")).getOrElse(OicpEndpointStatus.Unregistered),
      operatorId = Option(d.getString("operatorId")).getOrElse(""),
      operatorName = Option(d.getString("operatorName")).getOrElse(""),
      lastPatchJobOn = Option(d.getLong("lastPatchJobOn")).map(l => Instant.ofEpochMilli(l.longValue)),
      lastPatchJobResult = Option(d.getString("lastPatchJobResult")),
      createdOn = Instant.ofEpochMilli(Option(d.getLong("createdOn")).map(_.longValue).getOrElse(0L)),
      lastChangedOn = Instant.ofEpochMilli(Option(d.getLong("lastChangedOn")).map(_.longValue).getOrElse(0L))
    )

object MongoOicpEndpointRepository:

  val live: ZLayer[MongoDatabase, Nothing, OicpEndpointRepository] =
    ZLayer.fromFunction(new MongoOicpEndpointRepository(_))

// ── MongoTransactionReadRepository ───────────────────────────────────────

final class MongoTransactionReadRepository(db: MongoDatabase) extends TransactionReadRepository:

  override def findById(tid: TenantId, transactionId: Long): Task[Option[RoamingTransaction]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.transactions")
        .find(equal("_id", transactionId))
        .first()
        .toFuture()
    ).map(d => Option(d).map(docToTransaction))

  private def docToTransaction(d: Document): RoamingTransaction =
    // The "stop" field is a nested Document: { timestamp: Long, meterStop: Double, reason: String, ... }
    val stopDoc = Option(d.get("stop", classOf[Document]))
    RoamingTransaction(
      id = Option(d.getLong("_id")).map(_.longValue).getOrElse(0L),
      chargingStationId = Option(d.getString("chargeBoxID")).getOrElse(""),
      connectorId = Option(d.getInteger("connectorId")).map(_.intValue).getOrElse(0),
      evseId = Option(d.getInteger("evseId")).map(_.intValue),
      tagId = Option(d.getString("tagID")),
      userId = Option(d.getString("userID")),
      currency = Option(d.getString("priceUnit")),
      startDate = Instant.ofEpochMilli(Option(d.getLong("timestamp")).map(_.longValue).getOrElse(0L)),
      endDate = stopDoc.flatMap(sd => Option(sd.getLong("timestamp")).map(l => Instant.ofEpochMilli(l.longValue)))
        .getOrElse(Instant.EPOCH),
      meterStart = Option(d.getDouble("meterStart")).map(_.doubleValue).getOrElse(0.0),
      meterStop = stopDoc.flatMap(sd => Option(sd.getDouble("meterStop")).map(_.doubleValue)).getOrElse(0.0),
      totalConsumptionWh = Option(d.getDouble("currentTotalConsumption")).map(_.doubleValue).getOrElse(0.0),
      totalDurationSecs = Option(d.getLong("currentTotalDurationSecs")).map(_.longValue).getOrElse(0L),
      totalInactivitySecs = Option(d.getLong("currentTotalInactivitySecs")).map(_.longValue).getOrElse(0L),
      totalCost = Option(d.getDouble("currentCumulatedPrice")).map(_.doubleValue),
      siteId = Option(d.getString("siteID")),
      ocpiSessionId = Option(d.get("ocpiData", classOf[Document])).flatMap(o => Option(o.getString("sessionId"))),
      ocpiEvseId = Option(d.get("ocpiData", classOf[Document])).flatMap(o => Option(o.getString("evseId")))
    )

object MongoTransactionReadRepository:

  val live: ZLayer[MongoDatabase, Nothing, TransactionReadRepository] =
    ZLayer.fromFunction(new MongoTransactionReadRepository(_))

// ── MongoChargingStationLocationRepository ────────────────────────────────

final class MongoChargingStationLocationRepository(db: MongoDatabase)
    extends ChargingStationLocationRepository:

  override def findLocation(tid: TenantId, stationId: String): Task[Option[StationLocation]] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tid.value}.chargingstations")
        .find(equal("_id", stationId))
        .first()
        .toFuture()
    ).map(d => Option(d).map(docToLocation))

  private def docToLocation(d: Document): StationLocation =
    val addrDoc  = Option(d.get("address", classOf[Document]))
    val coordDoc = Option(d.get("coordinates", classOf[Document]))

    StationLocation(
      stationId = Option(d.getString("_id")).getOrElse(""),
      connectorType = Option(d.getList("connectors", classOf[Document]))
        .flatMap(cs => Option(cs.get(0)))
        .flatMap(c => Option(c.getString("type"))),
      address = addrDoc.flatMap(a => Option(a.getString("address1"))).getOrElse(""),
      city = addrDoc.flatMap(a => Option(a.getString("city"))).getOrElse(""),
      postalCode = addrDoc.flatMap(a => Option(a.getString("postalCode"))).getOrElse(""),
      country = addrDoc.flatMap(a => Option(a.getString("country"))).getOrElse("DEU"),
      latitude = coordDoc.flatMap(c => Option(c.getDouble("lat")).map(_.toString)).getOrElse("0.0"),
      longitude = coordDoc.flatMap(c => Option(c.getDouble("lng")).map(_.toString)).getOrElse("0.0"),
      siteName = Option(d.getString("siteID"))
    )

object MongoChargingStationLocationRepository:

  val live: ZLayer[MongoDatabase, Nothing, ChargingStationLocationRepository] =
    ZLayer.fromFunction(new MongoChargingStationLocationRepository(_))
