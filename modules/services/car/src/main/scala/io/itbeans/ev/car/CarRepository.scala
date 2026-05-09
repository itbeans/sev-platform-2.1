package io.itbeans.ev.car

import io.itbeans.ev.domain.{TenantId, UserId}
import io.itbeans.ev.mongo.TenantCollection
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.{FindOneAndReplaceOptions, Sorts}
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// CarRepository — MongoDB access for Car and CarCatalog entities.
//
// CarCatalog: stored in the DEFAULT tenant's `carcatalogs` / `carcatalogimages`
//   collections (not tenant-scoped). Uses MongoDatabase directly.
// Car: stored per-tenant in `{tenantId}.cars`.
//   Uses TenantCollection[Document] for the collection accessor.
//
// All upserts use findOneAndReplace(upsert=true) to preserve created metadata.
// ---------------------------------------------------------------------------

trait CarCatalogRepository:
  def upsert(catalog: CarCatalog): Task[Unit]
  def findById(id: Int): Task[Option[CarCatalog]]

  def findAll(
      search: Option[String] = None,
      makers: List[String] = Nil,
      limit: Int = 100,
      skip: Int = 0
  ): Task[List[CarCatalog]]
  def countAll(search: Option[String] = None, makers: List[String] = Nil): Task[Long]
  def distinctMakers: Task[List[String]]
  def upsertImage(carId: Int, imageHash: String, imageBase64: String): Task[Unit]
  def getImage(imageHash: String): Task[Option[String]]
  def deleteImages(carId: Int): Task[Unit]

trait CarRepository:
  def upsert(tenantId: TenantId, car: Car): Task[Unit]
  def findById(tenantId: TenantId, carId: String): Task[Option[Car]]
  def findByUser(tenantId: TenantId, userId: UserId): Task[List[Car]]

  def findAll(
      tenantId: TenantId,
      userId: Option[UserId] = None,
      search: Option[String] = None,
      limit: Int = 100,
      skip: Int = 0
  ): Task[List[Car]]
  def countAll(tenantId: TenantId, userId: Option[UserId] = None, search: Option[String] = None): Task[Long]
  def delete(tenantId: TenantId, carId: String): Task[Unit]
  def clearDefault(tenantId: TenantId, userId: UserId): Task[Unit]
  def setDefault(tenantId: TenantId, carId: String): Task[Unit]
  def findDefault(tenantId: TenantId, userId: UserId): Task[Option[Car]]

// ---------------------------------------------------------------------------
// Live implementations
// ---------------------------------------------------------------------------

final class MongoCarCatalogRepository(db: MongoDatabase) extends CarCatalogRepository:

  // CarCatalog is NOT tenant-scoped; it belongs to the default/global tenant.
  private val catalogs = db.getCollection[Document]("carcatalogs")
  private val images   = db.getCollection[Document]("carcatalogimages")

  override def upsert(catalog: CarCatalog): Task[Unit] =
    ZIO.fromFuture { _ =>
      val doc    = catalogToDoc(catalog)
      val filter = equal("_id", catalog.id)
      val opts   = FindOneAndReplaceOptions().upsert(true)
      catalogs.findOneAndReplace(filter, doc, opts).toFuture()
    }.unit

  override def findById(id: Int): Task[Option[CarCatalog]] =
    ZIO.fromFuture(_ => catalogs.find(equal("_id", id)).first().toFutureOption())
      .map(_.map(docToCatalog))

  override def findAll(
      search: Option[String],
      makers: List[String],
      limit: Int,
      skip: Int
  ): Task[List[CarCatalog]] =
    ZIO.fromFuture { _ =>
      val filter = buildCatalogFilter(search, makers)
      catalogs.find(filter).skip(skip).limit(limit).toFuture()
    }.map(_.map(docToCatalog).toList)

  override def countAll(search: Option[String], makers: List[String]): Task[Long] =
    ZIO.fromFuture(_ => catalogs.countDocuments(buildCatalogFilter(search, makers)).toFuture())

  override def distinctMakers: Task[List[String]] =
    ZIO.fromFuture(_ => catalogs.distinct[String]("vehicleMake").toFuture())
      .map(_.toList.sorted)

  override def upsertImage(carId: Int, imageHash: String, imageBase64: String): Task[Unit] =
    ZIO.fromFuture { _ =>
      val doc    = new Document("_id", imageHash).append("carID", carId).append("image", imageBase64)
      val filter = equal("_id", imageHash)
      val opts   = FindOneAndReplaceOptions().upsert(true)
      images.findOneAndReplace(filter, doc, opts).toFuture()
    }.unit

  override def getImage(imageHash: String): Task[Option[String]] =
    ZIO.fromFuture(_ => images.find(equal("_id", imageHash)).first().toFutureOption())
      .map(_.flatMap(d => Option(d.getString("image"))))

  override def deleteImages(carId: Int): Task[Unit] =
    ZIO.fromFuture(_ => images.deleteMany(equal("carID", carId)).toFuture()).unit

  private def buildCatalogFilter(search: Option[String], makers: List[String]): org.bson.conversions.Bson =
    val filters = scala.collection.mutable.ArrayBuffer.empty[org.bson.conversions.Bson]
    if makers.nonEmpty then filters += in("vehicleMake", makers*)
    search.foreach(s => filters += regex("vehicleModel", s, "i"))
    if filters.isEmpty then new Document()
    else and(filters.toSeq*)

  private def catalogToDoc(c: CarCatalog): Document =
    import scala.jdk.CollectionConverters._
    val d = new Document("_id", c.id)
      .append("vehicleMake", c.vehicleMake)
      .append("vehicleModel", c.vehicleModel)
      .append("createdOn", c.createdOn.toEpochMilli)
      .append("lastChangedOn", c.lastChangedOn.toEpochMilli)
    c.vehicleModelVersion.foreach(d.append("vehicleModelVersion", _))
    c.drivetrainPropulsion.foreach(d.append("drivetrainPropulsion", _))
    c.drivetrainPowerHP.foreach(d.append("drivetrainPowerHP", _))
    c.performanceAcceleration.foreach(d.append("performanceAcceleration", _))
    c.performanceTopspeed.foreach(d.append("performanceTopspeed", _))
    c.rangeReal.foreach(d.append("rangeReal", _))
    c.rangeWLTP.foreach(d.append("rangeWLTP", _))
    c.efficiencyReal.foreach(d.append("efficiencyReal", _))
    c.batteryCapacityUseable.foreach(d.append("batteryCapacityUseable", _))
    c.batteryCapacityFull.foreach(d.append("batteryCapacityFull", _))
    c.chargePlug.foreach(d.append("chargePlug", _))
    c.chargeStandardPower.foreach(d.append("chargeStandardPower", _))
    c.chargeStandardPhase.foreach(d.append("chargeStandardPhase", _))
    c.chargeStandardPhaseAmp.foreach(d.append("chargeStandardPhaseAmp", _))
    c.chargeStandardChargeTime.foreach(d.append("chargeStandardChargeTime", _))
    c.chargeStandardChargeSpeed.foreach(d.append("chargeStandardChargeSpeed", _))
    c.fastChargePlug.foreach(d.append("fastChargePlug", _))
    c.fastChargePowerMax.foreach(d.append("fastChargePowerMax", _))
    c.miscBody.foreach(d.append("miscBody", _))
    c.miscSegment.foreach(d.append("miscSegment", _))
    c.miscSeats.foreach(d.append("miscSeats", _))
    c.hash.foreach(d.append("hash", _))
    c.imagesHash.foreach(d.append("imagesHash", _))
    c.image.foreach(d.append("image", _))
    d.append("imageURLs", c.imageUrls.asJava)
    d

  private def docToCatalog(d: Document): CarCatalog =
    import scala.jdk.CollectionConverters._
    CarCatalog(
      id = d.getInteger("_id"),
      vehicleMake = d.getString("vehicleMake"),
      vehicleModel = d.getString("vehicleModel"),
      vehicleModelVersion = Option(d.getString("vehicleModelVersion")),
      drivetrainPropulsion = Option(d.getString("drivetrainPropulsion")),
      drivetrainPowerHP = Option(d.getInteger("drivetrainPowerHP")).map(_.intValue),
      performanceAcceleration = Option(d.getDouble("performanceAcceleration")).map(_.doubleValue),
      performanceTopspeed = Option(d.getInteger("performanceTopspeed")).map(_.intValue),
      rangeReal = Option(d.getInteger("rangeReal")).map(_.intValue),
      rangeWLTP = Option(d.getInteger("rangeWLTP")).map(_.intValue),
      efficiencyReal = Option(d.getDouble("efficiencyReal")).map(_.doubleValue),
      batteryCapacityUseable = Option(d.getDouble("batteryCapacityUseable")).map(_.doubleValue),
      batteryCapacityFull = Option(d.getDouble("batteryCapacityFull")).map(_.doubleValue),
      chargePlug = Option(d.getString("chargePlug")),
      chargeStandardPower = Option(d.getDouble("chargeStandardPower")).map(_.doubleValue),
      chargeStandardPhase = Option(d.getInteger("chargeStandardPhase")).map(_.intValue),
      chargeStandardPhaseAmp = Option(d.getDouble("chargeStandardPhaseAmp")).map(_.doubleValue),
      chargeStandardChargeTime = Option(d.getInteger("chargeStandardChargeTime")).map(_.intValue),
      chargeStandardChargeSpeed = Option(d.getDouble("chargeStandardChargeSpeed")).map(_.doubleValue),
      fastChargePlug = Option(d.getString("fastChargePlug")),
      fastChargePowerMax = Option(d.getDouble("fastChargePowerMax")).map(_.doubleValue),
      miscBody = Option(d.getString("miscBody")),
      miscSegment = Option(d.getString("miscSegment")),
      miscSeats = Option(d.getInteger("miscSeats")).map(_.intValue),
      imageUrls = Option(d.getList("imageURLs", classOf[String])).map(_.asScala.toList).getOrElse(Nil),
      image = Option(d.getString("image")),
      hash = Option(d.getString("hash")),
      imagesHash = Option(d.getString("imagesHash")),
      createdOn = Instant.ofEpochMilli(d.getLong("createdOn")),
      lastChangedOn = Instant.ofEpochMilli(d.getLong("lastChangedOn"))
    )

object MongoCarCatalogRepository:

  val live: ZLayer[MongoDatabase, Nothing, CarCatalogRepository] =
    ZLayer.fromFunction(new MongoCarCatalogRepository(_))

// ---------------------------------------------------------------------------
// MongoCarRepository — tenant-scoped car storage using TenantCollection
// ---------------------------------------------------------------------------

final class MongoCarRepository(db: MongoDatabase) extends CarRepository:

  private def col(tenantId: TenantId) =
    TenantCollection[Document]("cars", tenantId, db)

  override def upsert(tenantId: TenantId, car: Car): Task[Unit] =
    col(tenantId).replaceOne(equal("_id", car.id), carToDoc(car), upsert = true)

  override def findById(tenantId: TenantId, carId: String): Task[Option[Car]] =
    col(tenantId).findOne(equal("_id", carId)).map(_.map(docToCar))

  override def findByUser(tenantId: TenantId, userId: UserId): Task[List[Car]] =
    col(tenantId).find(equal("userID", userId.value)).runCollect.map(_.map(docToCar).toList)

  override def findAll(
      tenantId: TenantId,
      userId: Option[UserId],
      search: Option[String],
      limit: Int,
      skip: Int
  ): Task[List[Car]] =
    // For pagination we go through the raw collection
    ZIO.fromFuture { _ =>
      val coll   = db.getCollection[Document](s"${tenantId.value}.cars")
      val filter = buildCarFilter(userId, search)
      coll.find(filter).sort(Sorts.ascending("_id")).skip(skip).limit(limit).toFuture()
    }.map(_.map(docToCar).toList)

  override def countAll(tenantId: TenantId, userId: Option[UserId], search: Option[String]): Task[Long] =
    ZIO.fromFuture { _ =>
      val coll = db.getCollection[Document](s"${tenantId.value}.cars")
      coll.countDocuments(buildCarFilter(userId, search)).toFuture()
    }

  override def delete(tenantId: TenantId, carId: String): Task[Unit] =
    col(tenantId).deleteOne(equal("_id", carId))

  override def clearDefault(tenantId: TenantId, userId: UserId): Task[Unit] =
    col(tenantId).updateMany(
      and(equal("userID", userId.value), equal("default", true)),
      set("default", false)
    )

  override def setDefault(tenantId: TenantId, carId: String): Task[Unit] =
    col(tenantId).updateOne(equal("_id", carId), set("default", true))

  override def findDefault(tenantId: TenantId, userId: UserId): Task[Option[Car]] =
    col(tenantId)
      .findOne(and(equal("userID", userId.value), equal("default", true)))
      .map(_.map(docToCar))

  private def buildCarFilter(userId: Option[UserId], search: Option[String]): org.bson.conversions.Bson =
    val filters = scala.collection.mutable.ArrayBuffer.empty[org.bson.conversions.Bson]
    userId.foreach(uid => filters += equal("userID", uid.value))
    search.foreach(s => filters += regex("licensePlate", s, "i"))
    if filters.isEmpty then new Document()
    else and(filters.toSeq*)

  private def carToDoc(c: Car): Document =
    val d = new Document("_id", c.id)
      .append("userID", c.userId.value)
      .append("carCatalogID", c.carCatalogId)
      .append("type", c.carType.code)
      .append("default", c.default)
      .append("createdOn", c.createdOn.toEpochMilli)
      .append("lastChangedOn", c.lastChangedOn.toEpochMilli)
    c.vin.foreach(d.append("vin", _))
    c.licensePlate.foreach(d.append("licensePlate", _))
    c.converter.foreach { cv =>
      d.append(
        "converter",
        new Document()
          .append("powerWatts", cv.powerWatts)
          .append("amperagePerPhase", cv.amperagePerPhase)
          .append("numberOfPhases", cv.numberOfPhases)
          .append("type", cv.converterType.code)
      )
    }
    c.carConnectorData.foreach { cc =>
      val ccd = new Document("carConnectorID", cc.carConnectorId)
      cc.carConnectorMeterId.foreach(ccd.append("carConnectorMeterID", _))
      d.append("carConnectorData", ccd)
    }
    d

  private def docToCar(d: Document): Car =
    val converterDoc = Option(d.get("converter", classOf[Document]))
    val connectorDoc = Option(d.get("carConnectorData", classOf[Document]))
    Car(
      id = d.getString("_id"),
      vin = Option(d.getString("vin")),
      licensePlate = Option(d.getString("licensePlate")),
      carCatalogId = d.getInteger("carCatalogID"),
      userId = UserId(d.getString("userID")),
      carType = CarType.fromCode(d.getString("type")).getOrElse(CarType.Private),
      default = Option(d.getBoolean("default")).exists(_.booleanValue),
      converter = converterDoc.map { cv =>
        CarConverter(
          powerWatts = cv.getDouble("powerWatts"),
          amperagePerPhase = cv.getDouble("amperagePerPhase"),
          numberOfPhases = cv.getInteger("numberOfPhases"),
          converterType = ConverterType.fromCode(cv.getString("type")).getOrElse(ConverterType.Standard)
        )
      },
      carConnectorData = connectorDoc.map { cc =>
        CarConnectorData(
          carConnectorId = cc.getString("carConnectorID"),
          carConnectorMeterId = Option(cc.getString("carConnectorMeterID"))
        )
      },
      createdOn = Instant.ofEpochMilli(d.getLong("createdOn")),
      lastChangedOn = Instant.ofEpochMilli(d.getLong("lastChangedOn"))
    )

object MongoCarRepository:

  val live: ZLayer[MongoDatabase, Nothing, CarRepository] =
    ZLayer.fromFunction(new MongoCarRepository(_))
