package io.itbeans.ev.mongo

import io.itbeans.ev.domain.TenantId
import org.bson.conversions.Bson
import org.mongodb.scala._
import org.mongodb.scala.model._
import zio._
import zio.stream.ZStream

import scala.reflect.ClassTag

// ---------------------------------------------------------------------------
// MongoZIO — tenant-aware MongoDB access layer.
//
// The current ev-server uses collection-level multi-tenancy:
//   database.getCollection(s"${tenantId}.${collectionName}")
//
// This wrapper replicates that exact naming convention so that Scala services
// and the TypeScript monolith can share the same MongoDB instance during
// the migration period without any data changes.
// ---------------------------------------------------------------------------

/** Configuration for the MongoDB connection. */
case class MongoConfig(
    uri: String,
    databaseName: String,
    maxPoolSize: Int = 10
)

object MongoConfig:

  given zio.Config[MongoConfig] =
    (zio.Config.string("uri") zip zio.Config.string("databaseName") zip zio.Config.int("maxPoolSize").withDefault(10))
      .map { case (uri, db, pool) => MongoConfig(uri, db, pool) }

/** The live MongoDB client, scoped to the application lifetime. */
object MongoClientLayer:

  val live: ZLayer[MongoConfig, Throwable, MongoClient] =
    ZLayer.scoped {
      for
        config <- ZIO.service[MongoConfig]
        client <- ZIO.acquireRelease(
          ZIO.attempt(MongoClient(config.uri))
        )(c => ZIO.succeed(c.close()))
      yield client
    }

/** Access to a single MongoDB database. */
object MongoDatabaseLayer:

  val live: ZLayer[MongoClient & MongoConfig, Throwable, MongoDatabase] =
    ZLayer.fromZIO {
      for
        client <- ZIO.service[MongoClient]
        config <- ZIO.service[MongoConfig]
      yield client.getDatabase(config.databaseName)
    }

// ---------------------------------------------------------------------------
// TenantCollection — wraps a MongoCollection with tenant-prefixed naming.
// Usage:
//   val col = TenantCollection[Document]("transactions", tenantId, db)
//   col.findOne(Filters.eq("_id", id))
// ---------------------------------------------------------------------------

final class TenantCollection[T: ClassTag](
    private val collection: MongoCollection[T]
):

  /** Wrap a MongoDB Observable into a ZIO effect (single result). */
  def findOne(filter: Bson): Task[Option[T]] =
    ZIO.fromFuture(_ => collection.find(filter).first().toFutureOption())

  /** Wrap a MongoDB Observable into a ZStream (multiple results). */
  def find(filter: Bson): ZStream[Any, Throwable, T] =
    ZStream.fromIterableZIO(
      ZIO.fromFuture(_ => collection.find(filter).toFuture())
    )

  def find(filter: Bson, sort: Bson, limit: Int): ZStream[Any, Throwable, T] =
    ZStream.fromIterableZIO(
      ZIO.fromFuture(_ => collection.find(filter).sort(sort).limit(limit).toFuture())
    )

  def insertOne(document: T): Task[Unit] =
    ZIO.fromFuture(_ => collection.insertOne(document).toFuture()).unit

  def insertMany(documents: Seq[T]): Task[Unit] =
    ZIO.fromFuture(_ => collection.insertMany(documents).toFuture()).unit

  def replaceOne(filter: Bson, replacement: T, upsert: Boolean = false): Task[Unit] =
    val options = ReplaceOptions().upsert(upsert)
    ZIO.fromFuture(_ => collection.replaceOne(filter, replacement, options).toFuture()).unit

  def updateOne(filter: Bson, update: Bson): Task[Unit] =
    ZIO.fromFuture(_ => collection.updateOne(filter, update).toFuture()).unit

  def updateMany(filter: Bson, update: Bson): Task[Unit] =
    ZIO.fromFuture(_ => collection.updateMany(filter, update).toFuture()).unit

  def deleteOne(filter: Bson): Task[Unit] =
    ZIO.fromFuture(_ => collection.deleteOne(filter).toFuture()).unit

  def deleteMany(filter: Bson): Task[Long] =
    ZIO.fromFuture(_ => collection.deleteMany(filter).toFuture()).map(_.getDeletedCount)

  def countDocuments(filter: Bson): Task[Long] =
    ZIO.fromFuture(_ => collection.countDocuments(filter).toFuture())

  def aggregate[R: ClassTag](pipeline: Seq[Bson]): ZStream[Any, Throwable, R] =
    ZStream.fromIterableZIO(
      ZIO.fromFuture(_ => collection.aggregate[R](pipeline).toFuture())
    )

object TenantCollection:

  /** Resolve `{tenantId}.{collectionName}` — the canonical naming convention. */
  def apply[T: ClassTag](
      collectionName: String,
      tenantId: TenantId,
      db: MongoDatabase
  ): TenantCollection[T] =
    new TenantCollection[T](
      db.getCollection[T](s"${tenantId.value}.$collectionName")
    )

// ---------------------------------------------------------------------------
// Global collections (not tenant-prefixed) — only tenants and migrations.
// ---------------------------------------------------------------------------

object GlobalCollection:

  def apply[T: ClassTag](collectionName: String, db: MongoDatabase): TenantCollection[T] =
    new TenantCollection[T](db.getCollection[T](collectionName))
