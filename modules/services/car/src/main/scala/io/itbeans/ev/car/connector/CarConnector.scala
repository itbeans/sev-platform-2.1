package io.itbeans.ev.car.connector

import io.circe._
import io.circe.parser._
import io.itbeans.ev.car.{Car, CarConfig}
import zio._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpRequest.BodyPublishers
import java.time.{Duration, Instant}
import java.util.Base64

// ---------------------------------------------------------------------------
// CarConnector — live State-of-Charge (SoC) from third-party telematics.
//
// Three providers supported (mirrors TypeScript car-connector integrations):
//   • Mercedes — OAuth2 auth-code flow, SOC from vehicledata API
//   • Tronity  — OAuth2 client-credentials (JSON body), SOC from battery API
//   • Targa Telematics — OAuth2 client-credentials (form), SOC from digital-twin
//
// Token is cached per-connector in a ZIO Ref and refreshed 60 s before expiry.
// All HTTP uses java.net.http.HttpClient wrapped in ZIO.attemptBlocking.
// ---------------------------------------------------------------------------

trait CarConnector:
  /** Connector identifier, e.g. "mercedes" */
  def id: String

  /** Returns current state of charge (0–100) for the given car, if possible */
  def getCurrentSoC(car: Car): Task[Option[Int]]

// ---------------------------------------------------------------------------
// Mercedes
// ---------------------------------------------------------------------------

final class MercedesCarConnector(
    config: CarConfig,
    http: HttpClient,
    tokenRef: Ref[(String, Instant)]
) extends CarConnector:

  override val id = "mercedes"

  override def getCurrentSoC(car: Car): Task[Option[Int]] =
    car.vin match
      case None =>
        ZIO.logDebug(s"Mercedes SoC: no VIN for car ${car.id}").as(None)
      case Some(vin) =>
        (for
          token <- getOrRefreshToken
          cfg = config.connectors.mercedes
          url = s"${cfg.apiUrl}/vehicledata/v2/vehicles/$vin/resources/soc"
          respJson <- getJson(url, token)
          soc <- ZIO
            .fromEither(respJson.hcursor.downField("data").downField("soc").downField("value").as[Int])
            .mapError(e => new Exception(s"Mercedes SoC parse error: $e"))
        yield Some(soc))
          .catchAll { err =>
            ZIO.logError(s"Mercedes SoC failed for car=${car.id}: ${err.getMessage}").as(None)
          }

  private def getOrRefreshToken: Task[String] =
    tokenRef.get.flatMap { (token, expiresAt) =>
      if Instant.now().isBefore(expiresAt.minusSeconds(60)) then ZIO.succeed(token)
      else refreshToken
    }

  private def refreshToken: Task[String] =
    ZIO.attemptBlocking {
      val cfg  = config.connectors.mercedes
      val cred = Base64.getEncoder.encodeToString(s"${cfg.clientId}:${cfg.clientSecret}".getBytes)
      val req = HttpRequest
        .newBuilder(URI.create(s"${cfg.authenticationUrl}/as/token.oauth2"))
        .header("Authorization", s"Basic $cred")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(BodyPublishers.ofString("grant_type=client_credentials"))
        .timeout(Duration.ofSeconds(15))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap { body =>
      ZIO.fromEither(parse(body).flatMap(_.hcursor.downField("access_token").as[String]))
        .mapError(e => new Exception(s"Mercedes token error: $e"))
    }.flatMap { token =>
      tokenRef.set((token, Instant.now().plusSeconds(3500))).as(token)
    }

  private def getJson(url: String, bearer: String): Task[Json] =
    ZIO.attemptBlocking {
      val req = HttpRequest
        .newBuilder(URI.create(url))
        .header("Authorization", s"Bearer $bearer")
        .GET()
        .timeout(Duration.ofSeconds(15))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap(body => ZIO.fromEither(parse(body)).mapError(e => new Exception(e.message)))

// ---------------------------------------------------------------------------
// Tronity
// ---------------------------------------------------------------------------

final class TronityCarConnector(
    config: CarConfig,
    http: HttpClient,
    tokenRef: Ref[(String, Instant)]
) extends CarConnector:

  override val id = "tronity"

  override def getCurrentSoC(car: Car): Task[Option[Int]] =
    car.carConnectorData.flatMap(_.carConnectorMeterId) match
      case None =>
        ZIO.logDebug(s"Tronity SoC: no carConnectorMeterID for car ${car.id}").as(None)
      case Some(meterId) =>
        (for
          token <- getOrRefreshToken
          cfg = config.connectors.tronity
          url = s"${cfg.apiUrl}/v1/vehicles/$meterId/battery"
          respJson <- getJson(url, token)
          soc <- ZIO
            .fromEither(respJson.hcursor.downField("level").as[Int])
            .mapError(e => new Exception(s"Tronity SoC parse error: $e"))
        yield Some(soc))
          .catchAll { err =>
            ZIO.logError(s"Tronity SoC failed for car=${car.id}: ${err.getMessage}").as(None)
          }

  private def getOrRefreshToken: Task[String] =
    tokenRef.get.flatMap { (token, expiresAt) =>
      if Instant.now().isBefore(expiresAt.minusSeconds(60)) then ZIO.succeed(token)
      else refreshToken
    }

  private def refreshToken: Task[String] =
    ZIO.attemptBlocking {
      val cfg = config.connectors.tronity
      // Tronity uses JSON body (not form-encoded)
      val body =
        s"""{"client_id":"${cfg.clientId}","client_secret":"${cfg.clientSecret}","grant_type":"client_credentials"}"""
      val req = HttpRequest
        .newBuilder(URI.create(s"${cfg.apiUrl}/oauth/authentication"))
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(10))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap { body =>
      ZIO.fromEither(parse(body).flatMap(_.hcursor.downField("access_token").as[String]))
        .mapError(e => new Exception(s"Tronity token error: $e"))
    }.flatMap { token =>
      tokenRef.set((token, Instant.now().plusSeconds(3500))).as(token)
    }

  private def getJson(url: String, bearer: String): Task[Json] =
    ZIO.attemptBlocking {
      val req = HttpRequest
        .newBuilder(URI.create(url))
        .header("Authorization", s"Bearer $bearer")
        .GET()
        .timeout(Duration.ofSeconds(10))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap(body => ZIO.fromEither(parse(body)).mapError(e => new Exception(e.message)))

// ---------------------------------------------------------------------------
// Targa Telematics
// ---------------------------------------------------------------------------

final class TargaTelematicsCarConnector(
    config: CarConfig,
    http: HttpClient,
    tokenRef: Ref[(String, Instant)]
) extends CarConnector:

  override val id = "targaTelematics"

  override def getCurrentSoC(car: Car): Task[Option[Int]] =
    car.vin match
      case None =>
        ZIO.logDebug(s"Targa SoC: no VIN for car ${car.id}").as(None)
      case Some(vin) =>
        (for
          token <- getOrRefreshToken
          cfg = config.connectors.targaTelematics
          url = s"${cfg.apiUrl}/v1/digital-twin/vehicles/$vin"
          respJson <- getJson(url, token)
          soc <- ZIO
            .fromEither(
              respJson.hcursor
                .downField("vehicleDiagnostics")
                .downField("tractionBattery")
                .downField("batteryPercentage")
                .downField("value")
                .as[Int]
            )
            .mapError(e => new Exception(s"Targa SoC parse error: $e"))
        yield Some(soc))
          .catchAll { err =>
            ZIO.logError(s"Targa SoC failed for car=${car.id}: ${err.getMessage}").as(None)
          }

  private def getOrRefreshToken: Task[String] =
    tokenRef.get.flatMap { (token, expiresAt) =>
      if Instant.now().isBefore(expiresAt.minusSeconds(60)) then ZIO.succeed(token)
      else refreshToken
    }

  private def refreshToken: Task[String] =
    ZIO.attemptBlocking {
      val cfg  = config.connectors.targaTelematics
      val body = s"grant_type=client_credentials&client_id=${cfg.clientId}&client_secret=${cfg.clientSecret}"
      val req = HttpRequest
        .newBuilder(URI.create(s"${cfg.authenticationUrl}/auth/realms/platform-api/protocol/openid-connect/token"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .POST(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(10))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap { body =>
      ZIO.fromEither(parse(body).flatMap(_.hcursor.downField("access_token").as[String]))
        .mapError(e => new Exception(s"Targa token error: $e"))
    }.flatMap { token =>
      tokenRef.set((token, Instant.now().plusSeconds(3500))).as(token)
    }

  private def getJson(url: String, bearer: String): Task[Json] =
    ZIO.attemptBlocking {
      val req = HttpRequest
        .newBuilder(URI.create(url))
        .header("Authorization", s"Bearer $bearer")
        .GET()
        .timeout(Duration.ofSeconds(10))
        .build()
      http.send(req, HttpResponse.BodyHandlers.ofString()).body()
    }.flatMap(body => ZIO.fromEither(parse(body)).mapError(e => new Exception(e.message)))

// ---------------------------------------------------------------------------
// CarConnectorRegistry — routes SoC requests to the right connector
// ---------------------------------------------------------------------------

class CarConnectorRegistry(connectors: Map[String, CarConnector]):

  def getCurrentSoC(car: Car): Task[Option[Int]] =
    car.carConnectorData match
      case None =>
        ZIO.none
      case Some(data) =>
        connectors.get(data.carConnectorId) match
          case None =>
            ZIO.logWarning(s"No connector found for id=${data.carConnectorId}").as(None)
          case Some(connector) =>
            connector.getCurrentSoC(car)

object CarConnectorRegistry:

  val live: ZLayer[CarConfig, Nothing, CarConnectorRegistry] =
    ZLayer.fromZIO {
      for
        cfg         <- ZIO.service[CarConfig]
        mercedesRef <- Ref.make(("", Instant.EPOCH))
        tronityRef  <- Ref.make(("", Instant.EPOCH))
        targaRef    <- Ref.make(("", Instant.EPOCH))
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
        connectors = Map(
          "mercedes"        -> (new MercedesCarConnector(cfg, http, mercedesRef): CarConnector),
          "tronity"         -> (new TronityCarConnector(cfg, http, tronityRef): CarConnector),
          "targaTelematics" -> (new TargaTelematicsCarConnector(cfg, http, targaRef): CarConnector)
        ).filter { (k, _) =>
          k match
            case "mercedes"        => cfg.connectors.mercedes.enabled
            case "tronity"         => cfg.connectors.tronity.enabled
            case "targaTelematics" => cfg.connectors.targaTelematics.enabled
            case _                 => false
        }
      yield new CarConnectorRegistry(connectors)
    }
