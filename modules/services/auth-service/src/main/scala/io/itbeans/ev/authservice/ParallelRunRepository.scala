package io.itbeans.ev.authservice

import io.circe.parser.decode
import io.circe.syntax._
import org.bson.Document
import org.bson.json.{JsonMode, JsonWriterSettings}
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import zio._

import java.time.Instant

import CirceInstances.given

// ---------------------------------------------------------------------------
// ParallelRunRepository — persists token comparison records for analysis.
//
// Collection: {tenantId}.parallelruncomparisons
//
// `timestamp` is stored as an ISO-8601 string (UTC); lexicographic order
// matches chronological order for UTC instants, so Filters.gte/lte work.
// ---------------------------------------------------------------------------

trait ParallelRunRepository:
  def save(rec: ParallelRunComparison): Task[Unit]
  def findInRange(tenantId: String, from: Instant, to: Instant): Task[List[ParallelRunComparison]]
  def countInRange(tenantId: String, from: Instant, to: Instant): Task[Long]
  def countMatchedInRange(tenantId: String, from: Instant, to: Instant): Task[Long]

final class MongoParallelRunRepository(db: MongoDatabase) extends ParallelRunRepository:

  private val relaxed = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()

  private def col(tenantId: String) =
    db.getCollection[Document](s"$tenantId.parallelruncomparisons")

  def save(rec: ParallelRunComparison): Task[Unit] =
    ZIO.fromFuture { _ =>
      val bson = Document.parse(rec.asJson.noSpaces)
      bson.remove("id")
      bson.append("_id", rec.id)
      col(rec.tenantId).insertOne(bson).toFuture()
    }.unit

  def findInRange(tenantId: String, from: Instant, to: Instant): Task[List[ParallelRunComparison]] =
    ZIO.fromFuture { _ =>
      col(tenantId)
        .find(and(
          gte("timestamp", from.toString),
          lte("timestamp", to.toString)
        ))
        .toFuture()
    }.map { docs =>
      docs.flatMap(d => decode[ParallelRunComparison](remapId(d)).toOption).toList
    }

  def countInRange(tenantId: String, from: Instant, to: Instant): Task[Long] =
    ZIO.fromFuture { _ =>
      col(tenantId)
        .countDocuments(and(
          gte("timestamp", from.toString),
          lte("timestamp", to.toString)
        ))
        .toFuture()
    }

  def countMatchedInRange(tenantId: String, from: Instant, to: Instant): Task[Long] =
    ZIO.fromFuture { _ =>
      col(tenantId)
        .countDocuments(and(
          gte("timestamp", from.toString),
          lte("timestamp", to.toString),
          equal("matched", true)
        ))
        .toFuture()
    }

  private def remapId(doc: Document): String =
    io.circe.parser.parse(doc.toJson(relaxed)).fold(
      _ => doc.toJson(relaxed),
      j =>
        j.hcursor.downField("_id").focus match
          case Some(idVal) => j.mapObject(_.remove("_id").add("id", idVal)).noSpaces
          case None        => j.noSpaces
    )

object MongoParallelRunRepository:

  val live: ZLayer[MongoDatabase, Nothing, ParallelRunRepository] =
    ZLayer.fromFunction(new MongoParallelRunRepository(_))
