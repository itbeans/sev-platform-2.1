package io.itbeans.ev.smartcharging

import io.circe.{parser, Json}
import io.circe.syntax._
import zio._

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.Base64

// ---------------------------------------------------------------------------
// SapSmartChargingClient — HTTP client for the SAP Smart Charging Optimizer.
//
// Protocol:
//   POST {optimizerUrl}
//   Content-Type: application/json
//   Authorization: Basic base64("user:password")
//
// Request body: OptimizerRequest (JSON)
//   { "event": { "eventType": "Reoptimize" },
//     "state": { "fuseTree": {...}, "cars": [...], "carAssignments": [...],
//                "currentTimeSeconds": 42 } }
//
// Response body: OptimizerResult (JSON)
//   { "cars": [ { "id": 1, "currentPlan": [16.0, 16.0, ...], ... } ] }
//
// The `currentPlan` contains up to 96 amperage values (15-min slots, 24 h).
// ---------------------------------------------------------------------------

trait SapSmartChargingClient:
  def optimize(request: OptimizerRequest): Task[OptimizerResult]
  /** Returns true if the SAP optimizer endpoint is reachable (any HTTP response). */
  def checkConnection: Task[Boolean]

final class LiveSapSmartChargingClient(cfg: SmartChargingConfig) extends SapSmartChargingClient:

  private val http: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  // Basic auth: base64("user:password")
  private val authHeader: String =
    "Basic " + Base64.getEncoder.encodeToString(
      s"${cfg.optimizerUser}:${cfg.optimizerPassword}".getBytes("UTF-8")
    )

  def optimize(request: OptimizerRequest): Task[OptimizerResult] =
    ZIO.attemptBlocking {
      val body = request.asJson.noSpaces
      val req = HttpRequest.newBuilder(URI.create(cfg.optimizerUrl))
        .POST(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authHeader)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build()

      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      parseResponse(resp)
    }

  def checkConnection: Task[Boolean] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(cfg.optimizerUrl))
        .GET()
        .timeout(Duration.ofSeconds(5))
        .header("Authorization", authHeader)
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.discarding())
      resp.statusCode() < 500
    }.catchAll(_ => ZIO.succeed(false))

  private def parseResponse(resp: HttpResponse[String]): OptimizerResult =
    val body = resp.body()
    val json = parser.parse(body).getOrElse(
      throw new Exception(s"SAP optimizer non-JSON response (${resp.statusCode()}): $body")
    )
    if resp.statusCode() >= 400 then
      val msg = json.hcursor.downField("message").as[String].getOrElse(body)
      throw new Exception(s"SAP optimizer ${resp.statusCode()}: $msg")
    json.as[OptimizerResult].getOrElse(
      throw new Exception(s"SAP optimizer response does not match OptimizerResult schema: $body")
    )

object SapSmartChargingClient:

  val live: ZLayer[SmartChargingConfig, Nothing, SapSmartChargingClient] =
    ZLayer.fromFunction(new LiveSapSmartChargingClient(_))
