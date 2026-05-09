package io.itbeans.ev.asset

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
// AssetRepository — MongoDB access for Asset and AssetConnection entities.
//
// Collections:
//   {tenantId}.assets            — Asset documents (tenant-scoped)
//   {tenantId}.assetconnections  — Vendor connection configs (tenant-scoped)
//
// AssetRepository handles CRUD for assets.
// AssetConnectionRepository handles lookup by id for polling.
// ---------------------------------------------------------------------------

trait AssetRepository:
  def upsert(tenantId: TenantId, asset: Asset): Task[Unit]
  def findById(tenantId: TenantId, assetId: String): Task[Option[Asset]]

  def findAll(
      tenantId: TenantId,
      dynamicOnly: Boolean = false,
      limit: Int = 100,
      skip: Int = 0
  ): Task[List[Asset]]
  def countAll(tenantId: TenantId, dynamicOnly: Boolean = false): Task[Long]
  def delete(tenantId: TenantId, assetId: String): Task[Unit]

trait AssetConnectionRepository:
  def findById(tenantId: TenantId, connectionId: String): Task[Option[AssetConnection]]
  def upsert(tenantId: TenantId, connection: AssetConnection): Task[Unit]
  def delete(tenantId: TenantId, connectionId: String): Task[Unit]

// ── Asset repository implementation ─────────────────────────────────────────

final class MongoAssetRepository(db: MongoDatabase) extends AssetRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("assets", tenantId, db)

  override def upsert(tenantId: TenantId, asset: Asset): Task[Unit] =
    ZIO.fromFuture { _ =>
      val opts = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tenantId.value}.assets")
        .findOneAndReplace(equal("_id", asset.id), assetToDoc(asset), opts)
        .toFuture()
    }.unit

  override def findById(tenantId: TenantId, assetId: String): Task[Option[Asset]] =
    col(tenantId).findOne(equal("_id", assetId)).map(_.map(docToAsset))

  override def findAll(
      tenantId: TenantId,
      dynamicOnly: Boolean,
      limit: Int,
      skip: Int
  ): Task[List[Asset]] =
    ZIO.fromFuture { _ =>
      val filter = if dynamicOnly then equal("dynamicAsset", true) else new Document()
      db.getCollection[Document](s"${tenantId.value}.assets")
        .find(filter)
        .sort(Sorts.ascending("name"))
        .skip(skip)
        .limit(limit)
        .toFuture()
    }.map(_.map(docToAsset).toList)

  override def countAll(tenantId: TenantId, dynamicOnly: Boolean): Task[Long] =
    ZIO.fromFuture { _ =>
      val filter = if dynamicOnly then equal("dynamicAsset", true) else new Document()
      db.getCollection[Document](s"${tenantId.value}.assets")
        .countDocuments(filter)
        .toFuture()
    }

  override def delete(tenantId: TenantId, assetId: String): Task[Unit] =
    col(tenantId).deleteOne(equal("_id", assetId))

  // ── BSON conversion ────────────────────────────────────────────────────

  private def assetToDoc(a: Asset): Document =
    val c = a.currentConsumption
    val doc = new Document("_id", a.id)
      .append("name", a.name)
      .append("companyID", a.companyId)
      .append("siteAreaID", a.siteAreaId)
      .append("assetType", a.assetType.code)
      .append("fluctuationPercent", a.fluctuationPercent)
      .append("staticValueWatt", a.staticValueWatt)
      .append("coordinates", a.coordinates.asJava)
      .append("issuer", a.issuer)
      .append("dynamicAsset", a.dynamicAsset)
      .append("usesPushAPI", a.usesPushApi)
      .append("excludeFromSmartCharging", a.excludeFromSmartCharging)
      .append("createdOn", a.createdOn.toEpochMilli)
      .append("lastChangedOn", a.lastChangedOn.toEpochMilli)
      // Current consumption
      .append("currentInstantWatts", c.currentInstantWatts)
      .append("currentTotalConsumptionWh", c.currentTotalConsumptionWh)

    a.siteId.foreach(doc.append("siteID", _))
    a.image.foreach(doc.append("image", _))
    a.connectionId.foreach(doc.append("connectionID", _))
    a.meterId.foreach(doc.append("meterID", _))
    a.variationThresholdPercent.foreach(doc.append("variationThresholdPercent", _))
    a.powerWattsLastSmartCharging.foreach(doc.append("powerWattsLastSmartChargingRun", _))
    c.currentInstantWattsL1.foreach(doc.append("currentInstantWattsL1", _))
    c.currentInstantWattsL2.foreach(doc.append("currentInstantWattsL2", _))
    c.currentInstantWattsL3.foreach(doc.append("currentInstantWattsL3", _))
    c.currentInstantAmps.foreach(doc.append("currentInstantAmps", _))
    c.currentInstantAmpsL1.foreach(doc.append("currentInstantAmpsL1", _))
    c.currentInstantAmpsL2.foreach(doc.append("currentInstantAmpsL2", _))
    c.currentInstantAmpsL3.foreach(doc.append("currentInstantAmpsL3", _))
    c.currentInstantVolts.foreach(doc.append("currentInstantVolts", _))
    c.currentInstantVoltsL1.foreach(doc.append("currentInstantVoltsL1", _))
    c.currentInstantVoltsL2.foreach(doc.append("currentInstantVoltsL2", _))
    c.currentInstantVoltsL3.foreach(doc.append("currentInstantVoltsL3", _))
    c.currentConsumptionWh.foreach(doc.append("currentConsumptionWh", _))
    c.currentStateOfCharge.foreach(doc.append("currentStateOfCharge", _))
    c.lastConsumptionTimestamp.foreach(t => doc.append("lastConsumption", new Document("timestamp", t.toEpochMilli)))
    doc

  private def docToAsset(d: Document): Asset =
    val lastConsDoc = Option(d.get("lastConsumption", classOf[Document]))
    val lastConsTms = lastConsDoc.map(lc => Instant.ofEpochMilli(lc.getLong("timestamp").longValue))
    Asset(
      id = d.getString("_id"),
      name = d.getString("name"),
      companyId = d.getString("companyID"),
      siteId = Option(d.getString("siteID")),
      siteAreaId = d.getString("siteAreaID"),
      assetType = AssetType.fromCode(d.getString("assetType")).getOrElse(AssetType.Consumption),
      fluctuationPercent = Option(d.getDouble("fluctuationPercent")).map(_.doubleValue).getOrElse(0.0),
      staticValueWatt = Option(d.getDouble("staticValueWatt")).map(_.doubleValue).getOrElse(0.0),
      coordinates = Option(d.getList("coordinates", classOf[java.lang.Double]))
        .map(_.asScala.toList.map(_.doubleValue))
        .getOrElse(Nil),
      issuer = Option(d.getBoolean("issuer")).exists(_.booleanValue),
      image = Option(d.getString("image")),
      dynamicAsset = Option(d.getBoolean("dynamicAsset")).exists(_.booleanValue),
      usesPushApi = Option(d.getBoolean("usesPushAPI")).exists(_.booleanValue),
      connectionId = Option(d.getString("connectionID")),
      meterId = Option(d.getString("meterID")),
      excludeFromSmartCharging = Option(d.getBoolean("excludeFromSmartCharging")).exists(_.booleanValue),
      variationThresholdPercent = Option(d.getDouble("variationThresholdPercent")).map(_.doubleValue),
      powerWattsLastSmartCharging = Option(d.getDouble("powerWattsLastSmartChargingRun")).map(_.doubleValue),
      currentConsumption = AssetCurrentConsumption(
        currentInstantWatts = Option(d.getDouble("currentInstantWatts")).map(_.doubleValue).getOrElse(0.0),
        currentInstantWattsL1 = Option(d.getDouble("currentInstantWattsL1")).map(_.doubleValue),
        currentInstantWattsL2 = Option(d.getDouble("currentInstantWattsL2")).map(_.doubleValue),
        currentInstantWattsL3 = Option(d.getDouble("currentInstantWattsL3")).map(_.doubleValue),
        currentInstantAmps = Option(d.getDouble("currentInstantAmps")).map(_.doubleValue),
        currentInstantAmpsL1 = Option(d.getDouble("currentInstantAmpsL1")).map(_.doubleValue),
        currentInstantAmpsL2 = Option(d.getDouble("currentInstantAmpsL2")).map(_.doubleValue),
        currentInstantAmpsL3 = Option(d.getDouble("currentInstantAmpsL3")).map(_.doubleValue),
        currentInstantVolts = Option(d.getDouble("currentInstantVolts")).map(_.doubleValue),
        currentInstantVoltsL1 = Option(d.getDouble("currentInstantVoltsL1")).map(_.doubleValue),
        currentInstantVoltsL2 = Option(d.getDouble("currentInstantVoltsL2")).map(_.doubleValue),
        currentInstantVoltsL3 = Option(d.getDouble("currentInstantVoltsL3")).map(_.doubleValue),
        currentConsumptionWh = Option(d.getDouble("currentConsumptionWh")).map(_.doubleValue),
        currentTotalConsumptionWh = Option(d.getDouble("currentTotalConsumptionWh")).map(_.doubleValue).getOrElse(0.0),
        currentStateOfCharge = Option(d.getDouble("currentStateOfCharge")).map(_.doubleValue),
        lastConsumptionTimestamp = lastConsTms
      ),
      createdOn = Instant.ofEpochMilli(Option(d.getLong("createdOn")).map(_.longValue).getOrElse(0L)),
      lastChangedOn = Instant.ofEpochMilli(Option(d.getLong("lastChangedOn")).map(_.longValue).getOrElse(0L))
    )

object MongoAssetRepository:

  val live: ZLayer[MongoDatabase, Nothing, AssetRepository] =
    ZLayer.fromFunction(new MongoAssetRepository(_))

// ── AssetConnection repository ───────────────────────────────────────────────

final class MongoAssetConnectionRepository(db: MongoDatabase) extends AssetConnectionRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("assetconnections", tenantId, db)

  override def findById(tenantId: TenantId, connectionId: String): Task[Option[AssetConnection]] =
    col(tenantId).findOne(equal("_id", connectionId)).map(_.map(docToConnection))

  override def upsert(tenantId: TenantId, connection: AssetConnection): Task[Unit] =
    ZIO.fromFuture { _ =>
      val opts = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tenantId.value}.assetconnections")
        .findOneAndReplace(equal("_id", connection.id), connectionToDoc(connection), opts)
        .toFuture()
    }.unit

  override def delete(tenantId: TenantId, connectionId: String): Task[Unit] =
    col(tenantId).deleteOne(equal("_id", connectionId))

  private def connectionToDoc(c: AssetConnection): Document =
    val doc = new Document("_id", c.id)
      .append("name", c.name)
      .append("connectorType", c.connectorType.code)
      .append("url", c.url)
      .append("refreshIntervalMins", c.refreshIntervalMins)
    c.clientId.foreach(doc.append("clientId", _))
    c.clientSecret.foreach(doc.append("clientSecret", _))
    c.authenticationUrl.foreach(doc.append("authenticationUrl", _))
    c.user.foreach(doc.append("user", _))
    c.password.foreach(doc.append("password", _))
    doc

  private def docToConnection(d: Document): AssetConnection =
    AssetConnection(
      id = d.getString("_id"),
      name = d.getString("name"),
      connectorType = AssetConnectorType.fromCode(d.getString("connectorType"))
        .getOrElse(AssetConnectorType.Greencom),
      url = d.getString("url"),
      refreshIntervalMins = Option(d.getInteger("refreshIntervalMins")).map(_.intValue).getOrElse(5),
      clientId = Option(d.getString("clientId")),
      clientSecret = Option(d.getString("clientSecret")),
      authenticationUrl = Option(d.getString("authenticationUrl")),
      user = Option(d.getString("user")),
      password = Option(d.getString("password"))
    )

object MongoAssetConnectionRepository:

  val live: ZLayer[MongoDatabase, Nothing, AssetConnectionRepository] =
    ZLayer.fromFunction(new MongoAssetConnectionRepository(_))
