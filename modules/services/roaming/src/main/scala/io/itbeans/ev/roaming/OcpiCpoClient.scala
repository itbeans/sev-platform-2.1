package io.itbeans.ev.roaming

import io.circe.syntax._
import io.circe.parser.decode
import zio._

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

// ---------------------------------------------------------------------------
// OcpiCpoClient — HTTP client for outbound OCPI CPO operations.
//
// Pushes CDRs to registered EMSP partners after transaction stop.
// Uses the OCPI 2.1.1 protocol: JSON over HTTPS with token authentication.
//
// Authentication: `Authorization: Token {endpoint.token}` header.
// Response: OCPI response wrapper { status_code, data, ... }.
// ---------------------------------------------------------------------------

final class OcpiCpoClient:

  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  /**
   * Push a CDR to an EMSP partner.
   * Called after transaction stop for each registered OCPI EMSP endpoint.
   *
   * OCPI 2.1.1 CDR push: POST {endpoint.cdrsUrl}/{cdrId}
   */
  def pushCdr(endpoint: OcpiEndpoint, cdr: OcpiCdr): Task[Unit] =
    endpoint.cdrsUrl match
      case None =>
        ZIO.logWarning(s"OCPI endpoint ${endpoint.id} has no CDRs URL — skipping CDR push")
      case Some(cdrsUrl) =>
        val url = s"$cdrsUrl/${cdr.id}"
        put(url, cdr.asJson.noSpaces, endpoint.token)
          .flatMap { body =>
            ZIO.fromEither(decode[OcpiResponse[io.circe.Json]](body))
              .mapError(e => new Exception(s"Failed to parse OCPI CDR push response: $e"))
              .flatMap { resp =>
                if resp.status_code == 1000 then
                  ZIO.logInfo(s"OCPI CDR ${cdr.id} pushed to ${endpoint.name} (${endpoint.countryCode})")
                else
                  ZIO.logWarning(
                    s"OCPI CDR push to ${endpoint.name} returned status ${resp.status_code}: ${resp.status_message}"
                  )
              }
          }
          .catchAll { err =>
            ZIO.logError(s"OCPI CDR push to ${endpoint.id} failed: ${err.getMessage}")
          }

  /**
   * GET a token from an EMSP partner (OCPI Tokens module).
   * Used to check if a badge is authorized for roaming.
   */
  def getToken(
      endpoint: OcpiEndpoint,
      countryCode: String,
      partyId: String,
      tokenUid: String
  ): Task[Option[io.circe.Json]] =
    endpoint.tokensUrl match
      case None => ZIO.succeed(None)
      case Some(tokensUrl) =>
        val url = s"$tokensUrl/$countryCode/$partyId/$tokenUid"
        get(url, endpoint.token)
          .flatMap { body =>
            ZIO.fromEither(decode[OcpiResponse[io.circe.Json]](body))
              .map(_.data)
              .mapError(e => new Exception(s"Failed to parse OCPI token response: $e"))
          }
          .catchAll { err =>
            ZIO.logWarning(s"OCPI token lookup failed for $tokenUid: ${err.getMessage}") *>
              ZIO.succeed(None)
          }

  /**
   * Refresh credentials with an EMSP partner.
   * OCPI 2.1.1: PUT {endpoint.baseUrl}/ocpi/2.1.1/credentials with our updated token.
   * Partners call this to keep their copy of our token current.
   * We call it periodically as part of the background patch job.
   */
  def refreshCredentials(endpoint: OcpiEndpoint): Task[Unit] =
    val url = s"${endpoint.baseUrl}/ocpi/2.1.1/credentials"
    val body = io.circe.Json.obj(
      "token"  -> io.circe.Json.fromString(endpoint.localToken),
      "url"    -> io.circe.Json.fromString(s"${endpoint.baseUrl}/ocpi/cpo/2.1.1/versions"),
      "roles"  -> io.circe.Json.arr(
        io.circe.Json.obj(
          "role"         -> io.circe.Json.fromString("CPO"),
          "business_details" -> io.circe.Json.obj(
            "name" -> io.circe.Json.fromString(endpoint.name)
          ),
          "country_code" -> io.circe.Json.fromString(endpoint.countryCode),
          "party_id"     -> io.circe.Json.fromString(endpoint.partyId)
        )
      )
    ).noSpaces
    put(url, body, endpoint.token)
      .tapError(err =>
        ZIO.logWarning(s"OCPI credential refresh for ${endpoint.id} failed: ${err.getMessage}")
      )
      .unit
      .ignore

  // ── HTTP helpers ─────────────────────────────────────────────────────────

  private def get(url: String, token: String): Task[String] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(url))
        .GET()
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", s"Token $token")
        .header("Accept", "application/json")
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      if resp.statusCode() >= 400 then
        throw new Exception(s"HTTP ${resp.statusCode()} GET $url")
      resp.body()
    }

  private def put(url: String, body: String, token: String): Task[String] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(url))
        .PUT(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", s"Token $token")
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .build()
      val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
      if resp.statusCode() >= 400 then
        throw new Exception(s"HTTP ${resp.statusCode()} PUT $url")
      resp.body()
    }

object OcpiCpoClient:

  val live: ZLayer[Any, Nothing, OcpiCpoClient] =
    ZLayer.succeed(new OcpiCpoClient)
