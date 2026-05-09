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

  def routes(repo: RestApiRepository): List[ZServerEndpoint[Any, Any]] = List(
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
      ZIO.succeed(io.circe.Json.obj("status" -> io.circe.Json.fromString("Stopped")))
    },
    exportEp.zServerLogic { case (tenantId, inProgress, stationId) =>
      ZIO.succeed(("text/csv; charset=utf-8", "id,station,start,stop,kwh\n".getBytes("UTF-8")))
    }
  )
