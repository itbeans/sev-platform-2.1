package io.itbeans.ev.pricingservice

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
// PricingRepository — MongoDB access for PricingDefinition entities.
//
// Collection: {tenantId}.pricingdefinitions
//
// Indexed fields (mirrors TypeScript PricingStorage):
//   - entityID + entityType (tariff resolution queries)
//   - siteID (site-scoped tariff lookup)
//   - createdOn (sorted list)
// ---------------------------------------------------------------------------

trait PricingRepository:

  def findByEntity(
      tenantId: TenantId,
      entityType: PricingEntity,
      entityId: String
  ): Task[List[PricingDefinition]]

  def findByTenant(tenantId: TenantId): Task[List[PricingDefinition]]

  def findById(tenantId: TenantId, id: String): Task[Option[PricingDefinition]]

  def upsert(tenantId: TenantId, definition: PricingDefinition): Task[Unit]

  def delete(tenantId: TenantId, id: String): Task[Unit]

  def findAll(tenantId: TenantId, limit: Int = 100, skip: Int = 0): Task[List[PricingDefinition]]

  def countAll(tenantId: TenantId): Task[Long]

final class MongoPricingRepository(db: MongoDatabase) extends PricingRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("pricingdefinitions", tenantId, db)

  override def findByEntity(
      tenantId: TenantId,
      entityType: PricingEntity,
      entityId: String
  ): Task[List[PricingDefinition]] =
    col(tenantId)
      .find(and(equal("entityID", entityId), equal("entityType", entityType.code)))
      .runCollect
      .map(_.map(docToDefinition).toList)

  override def findByTenant(tenantId: TenantId): Task[List[PricingDefinition]] =
    col(tenantId)
      .find(equal("entityType", PricingEntity.Tenant.code))
      .runCollect
      .map(_.map(docToDefinition).toList)

  override def findById(tenantId: TenantId, id: String): Task[Option[PricingDefinition]] =
    col(tenantId).findOne(equal("_id", id)).map(_.map(docToDefinition))

  override def upsert(tenantId: TenantId, definition: PricingDefinition): Task[Unit] =
    ZIO.fromFuture { _ =>
      val doc    = definitionToDoc(definition)
      val filter = equal("_id", definition.id)
      val opts   = FindOneAndReplaceOptions().upsert(true)
      db.getCollection[Document](s"${tenantId.value}.pricingdefinitions")
        .findOneAndReplace(filter, doc, opts)
        .toFuture()
    }.unit

  override def delete(tenantId: TenantId, id: String): Task[Unit] =
    col(tenantId).deleteOne(equal("_id", id))

  override def findAll(tenantId: TenantId, limit: Int, skip: Int): Task[List[PricingDefinition]] =
    ZIO.fromFuture { _ =>
      db.getCollection[Document](s"${tenantId.value}.pricingdefinitions")
        .find()
        .sort(Sorts.descending("createdOn"))
        .skip(skip)
        .limit(limit)
        .toFuture()
    }.map(_.map(docToDefinition).toList)

  override def countAll(tenantId: TenantId): Task[Long] =
    ZIO.fromFuture(_ =>
      db.getCollection[Document](s"${tenantId.value}.pricingdefinitions")
        .countDocuments()
        .toFuture()
    )

  // ── BSON ↔ domain conversion ───────────────────────────────────────────

  private def definitionToDoc(d: PricingDefinition): Document =
    val doc = new Document("_id", d.id)
      .append("entityID", d.entityId)
      .append("entityType", d.entityType.code)
      .append("name", d.name)
      .append("currencyCode", d.currencyCode)
      .append("createdOn", d.createdOn.toEpochMilli)
      .append("lastChangedOn", d.lastChangedOn.toEpochMilli)
      .append("dimensions", dimensionsToDoc(d.dimensions))
    d.description.foreach(doc.append("description", _))
    d.siteId.foreach(doc.append("siteID", _))
    d.staticRestrictions.foreach(sr => doc.append("staticRestrictions", staticRestToDoc(sr)))
    d.restrictions.foreach(r => doc.append("restrictions", restrictionToDoc(r)))
    doc

  private def dimensionsToDoc(d: PricingDimensions): Document =
    val doc = new Document()
    d.flatFee.foreach(dim => doc.append("flatFee", dimToDoc(dim)))
    d.energy.foreach(dim => doc.append("energy", dimToDoc(dim)))
    d.chargingTime.foreach(dim => doc.append("chargingTime", dimToDoc(dim)))
    d.parkingTime.foreach(dim => doc.append("parkingTime", dimToDoc(dim)))
    doc

  private def dimToDoc(d: PricingDimension): Document =
    val doc = new Document("active", d.active).append("price", d.price.toDouble)
    d.stepSize.foreach(doc.append("stepSize", _))
    doc

  private def staticRestToDoc(r: PricingStaticRestriction): Document =
    val doc = new Document()
    r.validFrom.foreach(t => doc.append("validFrom", t.toEpochMilli))
    r.validTo.foreach(t => doc.append("validTo", t.toEpochMilli))
    r.connectorType.foreach(doc.append("connectorType", _))
    r.connectorPowerKw.foreach(doc.append("connectorPowerkW", _))
    doc

  private def restrictionToDoc(r: PricingRestriction): Document =
    val doc = new Document()
    if r.daysOfWeek.nonEmpty then
      doc.append("daysOfWeek", r.daysOfWeek.map(_.isoValue).asJava)
    r.timeFrom.foreach(doc.append("timeFrom", _))
    r.timeTo.foreach(doc.append("timeTo", _))
    r.minEnergyKwh.foreach(doc.append("minEnergyKWh", _))
    r.maxEnergyKwh.foreach(doc.append("maxEnergyKWh", _))
    r.minDurationSecs.foreach(doc.append("minDurationSecs", _))
    r.maxDurationSecs.foreach(doc.append("maxDurationSecs", _))
    doc

  private def docToDefinition(d: Document): PricingDefinition =
    val dimsDoc = Option(d.get("dimensions", classOf[Document])).getOrElse(new Document())
    PricingDefinition(
      id = d.getString("_id"),
      entityId = d.getString("entityID"),
      entityType = PricingEntity.fromCode(d.getString("entityType")).getOrElse(PricingEntity.Tenant),
      name = d.getString("name"),
      description = Option(d.getString("description")),
      siteId = Option(d.getString("siteID")),
      dimensions = docToDimensions(dimsDoc),
      staticRestrictions = Option(d.get("staticRestrictions", classOf[Document])).map(docToStaticRest),
      restrictions = Option(d.get("restrictions", classOf[Document])).map(docToRestriction),
      currencyCode = Option(d.getString("currencyCode")).getOrElse("EUR"),
      createdOn = Instant.ofEpochMilli(Option(d.getLong("createdOn")).map(_.longValue).getOrElse(0L)),
      lastChangedOn = Instant.ofEpochMilli(Option(d.getLong("lastChangedOn")).map(_.longValue).getOrElse(0L))
    )

  private def docToDimensions(d: Document): PricingDimensions =
    PricingDimensions(
      flatFee = Option(d.get("flatFee", classOf[Document])).map(docToDim),
      energy = Option(d.get("energy", classOf[Document])).map(docToDim),
      chargingTime = Option(d.get("chargingTime", classOf[Document])).map(docToDim),
      parkingTime = Option(d.get("parkingTime", classOf[Document])).map(docToDim)
    )

  private def docToDim(d: Document): PricingDimension =
    PricingDimension(
      active = Option(d.getBoolean("active")).exists(_.booleanValue),
      price = BigDecimal(Option(d.getDouble("price")).map(_.doubleValue).getOrElse(0.0)),
      stepSize = Option(d.getInteger("stepSize")).map(_.intValue)
    )

  private def docToStaticRest(d: Document): PricingStaticRestriction =
    PricingStaticRestriction(
      validFrom = Option(d.getLong("validFrom")).map(ms => Instant.ofEpochMilli(ms.longValue)),
      validTo = Option(d.getLong("validTo")).map(ms => Instant.ofEpochMilli(ms.longValue)),
      connectorType = Option(d.getString("connectorType")),
      connectorPowerKw = Option(d.getDouble("connectorPowerkW")).map(_.doubleValue)
    )

  private def docToRestriction(d: Document): PricingRestriction =
    val days = Option(d.getList("daysOfWeek", classOf[Integer]))
      .map(_.asScala.toList.flatMap(n => DayOfWeek.fromIso(n.intValue)))
      .getOrElse(Nil)
    PricingRestriction(
      daysOfWeek = days,
      timeFrom = Option(d.getString("timeFrom")),
      timeTo = Option(d.getString("timeTo")),
      minEnergyKwh = Option(d.getDouble("minEnergyKWh")).map(_.doubleValue),
      maxEnergyKwh = Option(d.getDouble("maxEnergyKWh")).map(_.doubleValue),
      minDurationSecs = Option(d.getInteger("minDurationSecs")).map(_.intValue),
      maxDurationSecs = Option(d.getInteger("maxDurationSecs")).map(_.intValue)
    )

object MongoPricingRepository:

  val live: ZLayer[MongoDatabase, Nothing, PricingRepository] =
    ZLayer.fromFunction(new MongoPricingRepository(_))
