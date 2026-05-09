package io.itbeans.ev.ocppprocessor

import io.itbeans.ev.domain._
import org.bson.Document
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.{ReplaceOptions, UpdateOptions}
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// ProcessorRepository — MongoDB read/write for ev-ocpp-processor.
//
// Collections (all prefixed with {tenantId}.):
//   chargingstations  — upserted on BootNotification, status updates
//   transactions      — created on TransactionEvent.Started, closed on Ended
//   consumptions      — appended on MeterValues / TransactionEvent.Updated
//
// Multi-tenancy: {tenantId}.{collectionName} naming convention preserved.
// ---------------------------------------------------------------------------

trait ProcessorRepository:
  // ── Stations ──────────────────────────────────────────────────────────────
  /** Upsert station document on BootNotification. */
  def upsertStation(tenantId: String, stationId: String, doc: Document): Task[Unit]

  /** Update connector status on StatusNotification. */
  def updateConnectorStatus(
      tenantId: String,
      stationId: String,
      connectorId: Int,
      status: String,
      timestamp: Instant
  ): Task[Unit]

  /** Update lastHeartbeat timestamp. */
  def updateHeartbeat(tenantId: String, stationId: String, timestamp: Instant): Task[Unit]

  /** Set arbitrary fields on a station document (used for firmware/diagnostics status). */
  def updateStationFields(tenantId: String, stationId: String, fields: Document): Task[Unit]

  // ── Transactions ──────────────────────────────────────────────────────────
  /** Insert a new transaction on TransactionStarted. */
  def createTransaction(tenantId: String, doc: Document): Task[Unit]

  /** Update running transaction fields (currentInstantWatts, consumption, price). */
  def updateTransaction(tenantId: String, transactionId: String, update: Document): Task[Unit]

  /** Close a transaction: set endDate, meterStop, stopReason, inProgress=false. */
  def closeTransaction(tenantId: String, transactionId: String, doc: Document): Task[Unit]

  /** Get a running transaction (to check if it exists before creating). */
  def getTransaction(tenantId: String, transactionId: String): Task[Option[Document]]

  /** Get a charging station document (for connector type / power lookup). */
  def getStation(tenantId: String, stationId: String): Task[Option[Document]]

  // ── Consumptions ──────────────────────────────────────────────────────────
  /** Append a meter-value reading row (also published to Kafka for ev-analytics). */
  def insertConsumption(tenantId: String, doc: Document): Task[Unit]

  /** Return the highest cumulatedKwh seen so far for a transaction (0.0 if none). */
  def getLastCumulatedKwh(tenantId: String, transactionId: String): Task[Double]

// ── MongoDB implementation ───────────────────────────────────────────────

final class MongoProcessorRepository(db: MongoDatabase) extends ProcessorRepository:

  private def col(tenantId: String, name: String) =
    db.getCollection[Document](s"$tenantId.$name")

  def upsertStation(tenantId: String, stationId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .replaceOne(
          equal("_id", stationId),
          doc.append("_id", stationId),
          ReplaceOptions().upsert(true)
        )
        .toFuture()
    }.unit

  def updateConnectorStatus(
      tenantId: String,
      stationId: String,
      connectorId: Int,
      status: String,
      timestamp: Instant
  ): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .updateOne(
          and(equal("_id", stationId), equal("connectors.connectorId", connectorId)),
          combine(
            set("connectors.$.status", status),
            set("lastChangedOn", java.util.Date.from(timestamp))
          )
        )
        .toFuture()
    }.unit

  def updateHeartbeat(tenantId: String, stationId: String, timestamp: Instant): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .updateOne(
          equal("_id", stationId),
          combine(
            set("lastHeartbeat", java.util.Date.from(timestamp)),
            set("lastChangedOn", java.util.Date.from(timestamp))
          )
        )
        .toFuture()
    }.unit

  def updateStationFields(tenantId: String, stationId: String, fields: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations")
        .updateOne(equal("_id", stationId), new Document("$set", fields))
        .toFuture()
    }.unit

  def createTransaction(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions").insertOne(doc).toFuture()
    }.unit

  def updateTransaction(tenantId: String, transactionId: String, update: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions")
        .updateOne(equal("_id", transactionId), new Document("$set", update))
        .toFuture()
    }.unit

  def closeTransaction(tenantId: String, transactionId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions")
        .updateOne(
          equal("_id", transactionId),
          combine(
            set("endDate", doc.get("endDate")),
            set("meterStop", doc.get("meterStop")),
            set("stopReason", doc.get("stopReason")),
            set("inProgress", false),
            set("lastChangedOn", doc.get("lastChangedOn"))
          )
        )
        .toFuture()
    }.unit

  def getTransaction(tenantId: String, transactionId: String): Task[Option[Document]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "transactions").find(equal("_id", transactionId)).headOption()
    }

  def getStation(tenantId: String, stationId: String): Task[Option[Document]] =
    ZIO.fromFuture { _ =>
      col(tenantId, "chargingstations").find(equal("_id", stationId)).headOption()
    }

  def insertConsumption(tenantId: String, doc: Document): Task[Unit] =
    ZIO.fromFuture { _ =>
      col(tenantId, "consumptions").insertOne(doc).toFuture()
    }.unit

  def getLastCumulatedKwh(tenantId: String, transactionId: String): Task[Double] =
    ZIO.fromFuture { _ =>
      col(tenantId, "consumptions")
        .find(equal("transactionId", transactionId.toLongOption.map(Long.box).getOrElse(transactionId)))
        .sort(descending("time"))
        .limit(1)
        .toFuture()
    }.map(docs =>
      docs.headOption.flatMap(d => Option(d.get("cumulatedKwh")).map(_.toString.toDoubleOption.getOrElse(0.0)))
        .getOrElse(0.0)
    )

object ProcessorRepository:

  val live: ZLayer[MongoDatabase, Nothing, ProcessorRepository] =
    ZLayer.fromFunction(new MongoProcessorRepository(_))
