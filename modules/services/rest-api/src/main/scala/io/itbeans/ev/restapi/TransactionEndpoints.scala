package io.itbeans.ev.restapi

import io.circe.generic.auto._
import io.itbeans.ev.domain._
import RestApiCodecs.given
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import zio._

// ---------------------------------------------------------------------------
// TransactionEndpoints — CRUD for Transactions.
// Mirrors: TransactionService.ts
// ---------------------------------------------------------------------------

case class TransactionsResponse(result: List[Transaction], count: Int)

object TransactionEndpoints:

  private val tenantHeader = header[String]("X-Tenant-ID")
  private val base         = endpoint.errorOut(stringBody)

  private val listEp = base.get
    .in("api" / "v1" / "Transactions")
    .in(tenantHeader)
    .in(query[Option[Boolean]]("InProgress"))
    .in(query[Option[Int]]("Limit").default(None))
    .in(query[Option[Int]]("Skip").default(None))
    .in(query[Option[String]]("ChargingStationID"))
    .in(query[Option[String]]("UserID"))
    .in(query[Option[Long]]("StartDateTime"))
    .in(query[Option[Long]]("EndDateTime"))
    .out(jsonBody[TransactionsResponse])

  private val getEp = base.get
    .in("api" / "v1" / "Transactions" / path[Long]("id"))
    .in(tenantHeader)
    .out(jsonBody[Transaction])

  private val deleteEp = base.delete
    .in("api" / "v1" / "Transactions" / path[Long]("id"))
    .in(tenantHeader)
    .out(statusCode(sttp.model.StatusCode.NoContent))

  /** Soft-stop an in-progress transaction (emergency). */
  private val stopEp = base.post
    .in("api" / "v1" / "Transactions" / path[Long]("id") / "Stop")
    .in(tenantHeader)
    .out(jsonBody[io.circe.Json])

  /** Export transaction list as CSV. */
  private val exportEp = base.get
    .in("api" / "v1" / "Transactions" / "Export")
    .in(tenantHeader)
    .in(query[Option[Boolean]]("InProgress"))
    .in(query[Option[String]]("ChargingStationID"))
    .out(header[String]("Content-Type"))
    .out(byteArrayBody)

  def routes(repo: RestApiRepository, gateway: OcppGatewayGrpcClient): List[ZServerEndpoint[Any, Any]] = List(
    listEp.zServerLogic { case (tenantId, inProgress, limit, skip, stationId, userId, from, to) =>
      repo.listTransactions(tenantId, limit.getOrElse(25), skip.getOrElse(0), inProgress, stationId, userId)
        .map(pr => TransactionsResponse(pr.result, pr.count))
        .mapError(_.getMessage)
    },
    getEp.zServerLogic { case (id, tenantId) =>
      repo.getTransaction(tenantId, id)
        .someOrFail(s"Transaction $id not found")
        .mapError { case e: Throwable => e.getMessage; case s: String => s }
    },
    deleteEp.zServerLogic { case (id, tenantId) =>
      repo.deleteTransaction(tenantId, id).mapError(_.getMessage).unit
    },
    stopEp.zServerLogic { case (id, tenantId) =>
      repo.getTransaction(tenantId, id)
        .flatMap {
          case None => ZIO.fail(s"Transaction $id not found")
          case Some(tx) =>
            val payload = io.circe.Json.obj("transactionId" -> io.circe.Json.fromLong(id)).noSpaces
            gateway
              .sendCommand(tenantId, tx.chargingStationId.value, "RemoteStopTransaction", payload)
              .as(io.circe.Json.obj("status" -> io.circe.Json.fromString("Accepted")))
              .catchAll { err =>
                ZIO.logWarning(s"[RestApi] RemoteStopTransaction failed for tx $id: ${err.getMessage}") *>
                  ZIO.fail(s"Gateway error for tx $id: ${err.getMessage}")
              }
        }
        .mapError(_.toString)
    },
    exportEp.zServerLogic { case (tenantId, inProgress, stationId) =>
      repo.listTransactions(tenantId, limit = 10000, skip = 0, inProgress, stationId, userId = None)
        .map { pr =>
          val header = "ID,ChargingStationID,ConnectorID,UserID,TagID,StartDate,EndDate,MeterStart,MeterStop,ConsumptionWh,DurationSecs,Price,Currency,StopReason,OcppVersion\n"
          val rows = pr.result.map { t =>
            List(
              t.id.value.toString,
              t.chargingStationId.value,
              t.connectorId.toString,
              t.userId.map(_.value).getOrElse(""),
              t.tagId.getOrElse(""),
              t.startDate.toString,
              t.endDate.map(_.toString).getOrElse(""),
              t.meterStart.toString,
              t.meterStop.map(_.toString).getOrElse(""),
              t.currentConsumptionWh.map(_.toString).getOrElse(""),
              t.currentTotalDurationSecs.map(_.toString).getOrElse(""),
              t.currentCumulatedPrice.map(_.toString).getOrElse(""),
              t.priceUnit.getOrElse(""),
              t.stopReason.getOrElse(""),
              t.ocppVersion.toString
            ).mkString(",")
          }
          ("text/csv; charset=utf-8", (header + rows.mkString("\n")).getBytes("UTF-8"))
        }
        .mapError(_.getMessage)
    }
  )
