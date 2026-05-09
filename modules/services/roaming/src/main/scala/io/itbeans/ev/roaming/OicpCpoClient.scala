package io.itbeans.ev.roaming

import io.circe.syntax._
import io.circe.parser.decode
import io.circe.Json
import zio._

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

// ---------------------------------------------------------------------------
// OicpCpoClient — HTTP client for outbound OICP 2.3.0 CPO operations.
//
// Pushes Charge Detail Records and EVSE status to Hubject.
//
// OICP 2.3.0 endpoints:
//   POST {baseUrl}/oicp/cdrs/v21/operators/{operatorId}/charge-detail-records
//   POST {baseUrl}/oicp/evsepull/v21/operators/{operatorId}/data-records
//   POST {baseUrl}/oicp/notificationmgmt/v11/operators/{operatorId}/evse-status-records
// ---------------------------------------------------------------------------

final class OicpCpoClient:

  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  /**
   * Push a Charge Detail Record to Hubject.
   * OICP 2.3.0: POST {baseUrl}/oicp/cdrs/v21/operators/{operatorId}/charge-detail-records
   */
  def pushCdr(endpoint: OicpEndpoint, cdr: OicpChargeDetailRecord): Task[Unit] =
    val url = s"${endpoint.baseUrl}/oicp/cdrs/v21/operators/${endpoint.operatorId}/charge-detail-records"
    // OICP wraps the CDR in an ActionType envelope
    val payload = Json.obj(
      "ActionType"          -> Json.fromString("fullLoad"),
      "ChargeDetailRecords" -> Json.arr(cdr.asJson)
    ).noSpaces
    post(url, payload)
      .flatMap { body =>
        ZIO.fromEither(decode[Json](body))
          .mapError(e => new Exception(s"OICP CDR push response parse error: $e"))
          .flatMap { json =>
            val code = json.hcursor.downField("Result").downField("StatusCode").downField("Code").as[String]
            code match
              case Right("000") =>
                ZIO.logInfo(s"OICP CDR ${cdr.SessionID} pushed to ${endpoint.name}")
              case Right(c) =>
                ZIO.logWarning(s"OICP CDR push to ${endpoint.name} returned code $c")
              case Left(_) =>
                ZIO.logWarning(s"OICP CDR push to ${endpoint.name}: could not parse status code")
          }
      }
      .catchAll { err =>
        ZIO.logError(s"OICP CDR push to ${endpoint.id} failed: ${err.getMessage}")
      }

  /**
   * Push EVSE status updates to Hubject.
   * Called when a connector status changes (Available, Occupied, etc.).
   * OICP 2.3.0: POST {baseUrl}/oicp/notificationmgmt/v11/operators/{operatorId}/evse-status-records
   */
  def pushEvseStatus(
      endpoint: OicpEndpoint,
      evseId: String,
      status: String // "Available" | "Occupied" | "Reserved" | "OutOfService"
  ): Task[Unit] =
    val url = s"${endpoint.baseUrl}/oicp/notificationmgmt/v11/operators/${endpoint.operatorId}/evse-status-records"
    val payload = Json.obj(
      "ActionType" -> Json.fromString("fullLoad"),
      "OperatorEvseStatus" -> Json.obj(
        "OperatorID"   -> Json.fromString(endpoint.operatorId),
        "OperatorName" -> Json.fromString(endpoint.operatorName),
        "EvseStatusRecord" -> Json.arr(Json.obj(
          "EvseID"     -> Json.fromString(evseId),
          "EvseStatus" -> Json.fromString(status)
        ))
      )
    ).noSpaces
    post(url, payload)
      .flatMap(_ => ZIO.logDebug(s"OICP EVSE status $evseId→$status pushed to ${endpoint.name}"))
      .catchAll { err =>
        ZIO.logError(s"OICP EVSE status push to ${endpoint.id} failed: ${err.getMessage}")
      }

  // ── HTTP helper ───────────────────────────────────────────────────────────

  private def post(url: String, body: String): Task[String] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(url))
        .POST(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      if resp.statusCode() >= 400 then
        throw new Exception(s"HTTP ${resp.statusCode()} POST $url")
      resp.body()
    }

object OicpCpoClient:

  val live: ZLayer[Any, Nothing, OicpCpoClient] =
    ZLayer.succeed(new OicpCpoClient)
