package io.itbeans.ev.asset.connector

import io.circe.Json
import io.circe.parser.parse
import io.itbeans.ev.asset.{AssetConnection, AssetConnectorType, AssetMeasurement, AssetType}
import zio._

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration, Instant}
import java.util.Base64

// ---------------------------------------------------------------------------
// AssetConnector — vendor API clients for energy asset consumption polling.
//
// Five vendors are supported (mirrors TypeScript AssetIntegration subclasses):
//   GreencomConnector  — OAuth2 client credentials
//   SchneiderConnector — Password grant (form-encoded)
//   IOThinkConnector   — Password grant + in-memory token cache
//   LacroixConnector   — HTTP Basic auth login
//   WitConnector       — OAuth2 client credentials + in-memory token cache
//
// Each connector implements fetchConsumptions(connection, asset, manualCall)
// which returns a list of AssetMeasurement for the given asset/meter.
//
// Token caching (IOThink, WIT): Ref-backed in-memory map keyed by connectionId.
// Tokens are refreshed 60 seconds before expiry.
// ---------------------------------------------------------------------------

trait AssetConnector:
  def connectorType: AssetConnectorType

  /**
   * Fetch consumption measurements for one asset.
   * @param connection  vendor connection config (url, credentials, refresh interval)
   * @param meterId     vendor-specific meter/device ID from the Asset
   * @param assetType   asset type (affects sign of production assets)
   * @param since       last consumed timestamp — only return data after this
   * @param manualCall  true when triggered manually (returns single latest point)
   */
  def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]]

// ── Shared HTTP utility ───────────────────────────────────────────────────────

private val SharedHttpClient: HttpClient =
  HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()

private def formBody(params: (String, String)*): HttpRequest.BodyPublisher =
  val body = params.map { case (k, v) => s"$k=${java.net.URLEncoder.encode(v, "UTF-8")}" }.mkString("&")
  BodyPublishers.ofString(body)

private def get(url: String, headers: Map[String, String] = Map.empty): Task[String] =
  ZIO.attemptBlocking {
    val builder = HttpRequest.newBuilder(URI.create(url))
      .GET()
      .timeout(Duration.ofSeconds(30))
    headers.foreach { case (k, v) => builder.header(k, v) }
    val resp = SharedHttpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    if resp.statusCode() >= 400 then
      throw new Exception(s"HTTP ${resp.statusCode()} for GET $url")
    resp.body()
  }

private def post(url: String, body: HttpRequest.BodyPublisher, headers: Map[String, String] = Map.empty): Task[String] =
  ZIO.attemptBlocking {
    val builder = HttpRequest.newBuilder(URI.create(url))
      .POST(body)
      .timeout(Duration.ofSeconds(30))
    headers.foreach { case (k, v) => builder.header(k, v) }
    val resp = SharedHttpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    if resp.statusCode() >= 400 then
      throw new Exception(s"HTTP ${resp.statusCode()} for POST $url")
    resp.body()
  }

// ── Greencom connector ────────────────────────────────────────────────────────

/**
 * Greencom Networks: OAuth2 client credentials → GET site meter data.
 * API: {url}/site-api/{meterID}?withEnergy=true&withPower=true&from=T&to=T&step=PT1M
 * Response: JSON with energy.data[].value (Wh) and power.data[].value (W)
 */
final class GreencomConnector extends AssetConnector:
  override val connectorType = AssetConnectorType.Greencom

  override def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]] =
    for
      clientId <- ZIO.fromOption(connection.clientId)
        .orElseFail(new Exception("Greencom: clientId missing"))
      clientSecret <- ZIO.fromOption(connection.clientSecret)
        .orElseFail(new Exception("Greencom: clientSecret missing"))
      token <- fetchToken(connection.url, clientId, clientSecret)
      from = since.getOrElse(Instant.now().minusSeconds(90)).toString
      to   = Instant.now().toString
      url = s"${connection.url}/site-api/$meterId?withEnergy=true&withPower=true" +
        s"&from=$from&to=$to&step=PT1M"
      body <- get(url, Map("Authorization" -> s"Bearer $token"))
      measurements <- ZIO.fromEither(parseGreencom(body, assetType))
        .mapError(e => new Exception(s"Greencom parse: $e"))
    yield measurements

  private def fetchToken(url: String, clientId: String, clientSecret: String): Task[String] =
    post(
      s"$url/authentication-api/tokens",
      formBody("grant_type" -> "client_credentials", "client_id" -> clientId, "client_secret" -> clientSecret),
      Map("Content-Type"    -> "application/x-www-form-urlencoded")
    ).flatMap { body =>
      ZIO.fromEither(
        parse(body).flatMap(j => j.hcursor.downField("access_token").as[String])
      ).mapError(e => new Exception(s"Greencom token parse: $e"))
    }

  private def parseGreencom(body: String, assetType: AssetType): Either[String, List[AssetMeasurement]] =
    parse(body).left.map(_.message).flatMap { json =>
      val cursor       = json.hcursor
      val isProduction = assetType == AssetType.Production || assetType == AssetType.ConsumptionProduction

      val energyPoints = cursor.downField("data").downField("energy").downField("data")
        .as[List[Json]].getOrElse(Nil)
      val powerPoints = cursor.downField("data").downField("power").downField("data")
        .as[List[Json]].getOrElse(Nil)
      val socPoints = cursor.downField("data").downField("percent").downField("soc").downField("data")
        .as[List[Json]].getOrElse(Nil)

      val measurements = energyPoints.zipWithIndex.flatMap { case (ep, i) =>
        for
          ts     <- ep.hcursor.downField("timestamp").as[Long].toOption
          energy <- ep.hcursor.downField("value").as[Double].toOption
          power = powerPoints.lift(i).flatMap(_.hcursor.downField("value").as[Double].toOption).getOrElse(0.0)
          soc   = socPoints.lift(i).flatMap(_.hcursor.downField("value").as[Double].toOption)
          sign  = if isProduction then -1.0 else 1.0
        yield AssetMeasurement(
          timestamp = Instant.ofEpochMilli(ts),
          instantWatts = power * sign,
          instantWattsL1 = None,
          instantWattsL2 = None,
          instantWattsL3 = None,
          instantAmps = None,
          instantAmpsL1 = None,
          instantAmpsL2 = None,
          instantAmpsL3 = None,
          instantVolts = None,
          instantVoltsL1 = None,
          instantVoltsL2 = None,
          instantVoltsL3 = None,
          consumptionWh = energy * sign,
          stateOfCharge = soc
        )
      }
      Right(measurements)
    }

// ── Schneider connector ───────────────────────────────────────────────────────

/**
 * Schneider Electric: password grant → GET current meter properties.
 * API: {url}/{meterID}
 * Response: Array of {Name, Value} property objects.
 */
final class SchneiderConnector extends AssetConnector:
  override val connectorType = AssetConnectorType.Schneider

  override def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]] =
    for
      user     <- ZIO.fromOption(connection.user).orElseFail(new Exception("Schneider: user missing"))
      password <- ZIO.fromOption(connection.password).orElseFail(new Exception("Schneider: password missing"))
      token    <- fetchToken(connection.url, user, password)
      url = s"${connection.url}/$meterId"
      body <- get(url, Map("Authorization" -> s"Bearer $token"))
      result <- ZIO.fromEither(parseSchneider(body, assetType))
        .mapError(e => new Exception(s"Schneider parse: $e"))
    yield result

  private def fetchToken(url: String, user: String, password: String): Task[String] =
    post(
      s"$url/GetToken",
      formBody("grant_type" -> "password", "username" -> user, "password" -> password),
      Map("Content-Type"    -> "application/x-www-form-urlencoded")
    ).flatMap { body =>
      ZIO.fromEither(
        parse(body).flatMap(j => j.hcursor.downField("access_token").as[String])
      ).mapError(e => new Exception(s"Schneider token: $e"))
    }

  private def parseSchneider(body: String, assetType: AssetType): Either[String, List[AssetMeasurement]] =
    parse(body).left.map(_.message).flatMap { json =>
      json.as[List[Json]].left.map(_.message).map { props =>
        val propMap = props.flatMap { p =>
          for
            name  <- p.hcursor.downField("Name").as[String].toOption
            value <- p.hcursor.downField("Value").as[Double].toOption
          yield name -> value
        }.toMap

        val sign         = if assetType == AssetType.Production then -1.0 else 1.0
        val energyKwh    = propMap.getOrElse("Energie_Active", 0.0)
        val powerTotalKw = propMap.getOrElse("PActive_Tot", 0.0)
        val powerL1Kw    = propMap.getOrElse("PActive_Ph1", 0.0)
        val powerL2Kw    = propMap.getOrElse("PActive_Ph2", 0.0)
        val powerL3Kw    = propMap.getOrElse("PActive_Ph3", 0.0)
        val ampsL1       = propMap.get("I1")
        val ampsL2       = propMap.get("I2")
        val ampsL3       = propMap.get("I3")
        val voltsL1      = propMap.get("L1_N")
        val voltsL2      = propMap.get("L2_N")
        val voltsL3      = propMap.get("L3_N")
        val voltsAvg     = propMap.get("LN_Moy")

        List(AssetMeasurement(
          timestamp = Instant.now(),
          instantWatts = powerTotalKw * 1000 * sign,
          instantWattsL1 = Some(powerL1Kw * 1000 * sign),
          instantWattsL2 = Some(powerL2Kw * 1000 * sign),
          instantWattsL3 = Some(powerL3Kw * 1000 * sign),
          instantAmps = ampsL1.zip(ampsL2).zip(ampsL3).map { case ((l1, l2), l3) => (l1 + l2 + l3) / 3 },
          instantAmpsL1 = ampsL1,
          instantAmpsL2 = ampsL2,
          instantAmpsL3 = ampsL3,
          instantVolts = voltsAvg,
          instantVoltsL1 = voltsL1,
          instantVoltsL2 = voltsL2,
          instantVoltsL3 = voltsL3,
          consumptionWh = energyKwh * 1000 * sign,
          stateOfCharge = None
        ))
      }
    }

// ── IOThink connector ─────────────────────────────────────────────────────────

/**
 * IOThink: password grant + token cache → GET historical tag logs.
 * API: {url}/{meterID}&startTime={secondsSince2000-01-01}
 * Response: {historics: [{tagReference, logs: [{timestamp, value}]}]}
 * Timestamp reference: seconds since 2000-01-01T00:00:00Z.
 */
final class IOThinkConnector(tokenCache: Ref[Map[String, (String, Instant)]]) extends AssetConnector:
  override val connectorType = AssetConnectorType.IOThink

  private val EpochOffset2000 = Instant.parse("2000-01-01T00:00:00Z").getEpochSecond

  override def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]] =
    for
      user     <- ZIO.fromOption(connection.user).orElseFail(new Exception("IOThink: user missing"))
      password <- ZIO.fromOption(connection.password).orElseFail(new Exception("IOThink: password missing"))
      token    <- getOrRefreshToken(connection.id, connection.url, user, password)
      startTs = since.map(_.getEpochSecond - EpochOffset2000).getOrElse(0L)
      url     = s"${connection.url}/$meterId&startTime=$startTs"
      body <- get(url, Map("Authorization" -> s"Bearer $token"))
      result <- ZIO.fromEither(parseIOThink(body, assetType))
        .mapError(e => new Exception(s"IOThink parse: $e"))
    yield result

  private def getOrRefreshToken(connectionId: String, url: String, user: String, password: String): Task[String] =
    for
      cached <- tokenCache.get.map(_.get(connectionId))
      token <- cached match
        case Some((t, expiry)) if Instant.now().isBefore(expiry.minusSeconds(60)) => ZIO.succeed(t)
        case _ =>
          for
            resp <- post(
              s"$url/token",
              formBody("grant_type" -> "password", "username" -> user, "password" -> password),
              Map("Content-Type"    -> "application/x-www-form-urlencoded")
            )
            j <- ZIO.fromEither(parse(resp)).mapError(e => new Exception(s"IOThink token: $e"))
            t <- ZIO.fromEither(j.hcursor.downField("access_token").as[String]).mapError(new Exception(_))
            expiresIn = j.hcursor.downField("expires_in").as[Long].getOrElse(3600L)
            expiry    = Instant.now().plusSeconds(expiresIn)
            _ <- tokenCache.update(_.updated(connectionId, (t, expiry)))
          yield t
    yield token

  private def parseIOThink(body: String, assetType: AssetType): Either[String, List[AssetMeasurement]] =
    parse(body).left.map(_.message).flatMap { json =>
      val historics    = json.hcursor.downField("historics").as[List[Json]].getOrElse(Nil)
      val isProduction = assetType == AssetType.Production
      val sign         = if isProduction then -1.0 else 1.0

      // Build tag → timestamp → value map
      val tagData = historics.flatMap { h =>
        val tag  = h.hcursor.downField("tagReference").as[String].getOrElse("")
        val logs = h.hcursor.downField("logs").as[List[Json]].getOrElse(Nil)
        val points = logs.flatMap { l =>
          for
            ts <- l.hcursor.downField("timestamp").as[Long].toOption
            v  <- l.hcursor.downField("value").as[Double].toOption
          yield ts -> v
        }
        points.map { case (ts, v) => (ts, tag, v) }
      }

      // Group by timestamp, sorted chronologically
      val byTimestamp     = tagData.groupBy(_._1).view.mapValues(_.map(t => t._2 -> t._3).toMap).toMap
      val EpochOffset2000 = Instant.parse("2000-01-01T00:00:00Z").getEpochSecond

      val measurements = byTimestamp.toList.sortBy(_._1).map { case (tsSecs, tags) =>
        AssetMeasurement(
          timestamp = Instant.ofEpochSecond(tsSecs + EpochOffset2000),
          instantWatts = tags.getOrElse("io-pow-active", 0.0) * 1000 * sign,
          instantWattsL1 = tags.get("io-pow-l1").map(_ * 1000 * sign),
          instantWattsL2 = tags.get("io-pow-l2").map(_ * 1000 * sign),
          instantWattsL3 = tags.get("io-pow-l3").map(_ * 1000 * sign),
          instantAmps = None,
          instantAmpsL1 = None,
          instantAmpsL2 = None,
          instantAmpsL3 = None,
          instantVolts = None,
          instantVoltsL1 = None,
          instantVoltsL2 = None,
          instantVoltsL3 = None,
          consumptionWh = tags.getOrElse("io-energy-input", 0.0) * 1000 * sign,
          stateOfCharge = tags.get("io-soc")
        )
      }
      Right(measurements)
    }

// ── Lacroix connector ─────────────────────────────────────────────────────────

/**
 * Lacroix Energy: HTTP Basic auth login → GET power data.
 * API: {url}/{meterID}/getPowerData?period={period}&group_by=1m
 * Response: {data: [{date, powerApparentConsumedTotal, powerApparentConsumed1/2/3}]}
 * Note: Returns apparent power in VA. Subtracting EV load is a TODO
 *       (requires access to site-area charge transactions in same step).
 */
final class LacroixConnector extends AssetConnector:
  override val connectorType = AssetConnectorType.Lacroix

  override def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]] =
    for
      user     <- ZIO.fromOption(connection.user).orElseFail(new Exception("Lacroix: user missing"))
      password <- ZIO.fromOption(connection.password).orElseFail(new Exception("Lacroix: password missing"))
      _        <- login(connection.url, user, password)
      period = selectPeriod(since)
      url    = s"${connection.url}/$meterId/getPowerData?period=$period&group_by=1m"
      body <- get(url)
      result <- ZIO.fromEither(parseLacroix(body, since))
        .mapError(e => new Exception(s"Lacroix parse: $e"))
    yield result

  private def login(url: String, user: String, password: String): Task[Unit] =
    post(
      s"$url/login",
      formBody("username" -> user, "password" -> password),
      Map("Content-Type"  -> "application/x-www-form-urlencoded")
    ).unit

  private def selectPeriod(since: Option[Instant]): String =
    since match
      case None => "last"
      case Some(ts) =>
        val ageSeconds = Instant.now().getEpochSecond - ts.getEpochSecond
        if ageSeconds < 120 then "last"
        else if ageSeconds < 600 then "5m"
        else if ageSeconds < 7200 then "1h"
        else "day"

  private def parseLacroix(body: String, since: Option[Instant]): Either[String, List[AssetMeasurement]] =
    parse(body).left.map(_.message).flatMap { json =>
      json.hcursor.downField("data").as[List[Json]].left.map(_.message).map { dataPoints =>
        dataPoints.flatMap { dp =>
          for
            dateStr <- dp.hcursor.downField("date").as[String].toOption
            ts = scala.util.Try(Instant.parse(dateStr)).toOption.getOrElse(Instant.now())
            if since.forall(s => !ts.isBefore(s)) // skip data older than `since`
            totalVa <- dp.hcursor.downField("powerApparentConsumedTotal").as[Double].toOption
          yield
            val vaL1 = dp.hcursor.downField("powerApparentConsumed1").as[Double].toOption
            val vaL2 = dp.hcursor.downField("powerApparentConsumed2").as[Double].toOption
            val vaL3 = dp.hcursor.downField("powerApparentConsumed3").as[Double].toOption
            AssetMeasurement(
              timestamp = ts,
              instantWatts = totalVa, // apparent power ≈ active power for resistive loads
              instantWattsL1 = vaL1,
              instantWattsL2 = vaL2,
              instantWattsL3 = vaL3,
              instantAmps = None,
              instantAmpsL1 = None,
              instantAmpsL2 = None,
              instantAmpsL3 = None,
              instantVolts = None,
              instantVoltsL1 = None,
              instantVoltsL2 = None,
              instantVoltsL3 = None,
              consumptionWh = 0.0, // not directly available; caller derives from power × time
              stateOfCharge = None
            )
        }
      }
    }

// ── WIT connector ─────────────────────────────────────────────────────────────

/**
 * WIT Datacenter: OAuth2 (user+pass+client_id+client_secret) + token cache
 * → GET time-series datasets.
 * API: {url}/{meterID}?From={iso8601}
 * Response: [{T, V, wType}] where V is power in kW.
 */
final class WitConnector(tokenCache: Ref[Map[String, (String, Instant)]]) extends AssetConnector:
  override val connectorType = AssetConnectorType.Wit

  override def fetchConsumptions(
      connection: AssetConnection,
      meterId: String,
      assetType: AssetType,
      since: Option[Instant],
      manualCall: Boolean
  ): Task[List[AssetMeasurement]] =
    for
      authUrl <- ZIO.fromOption(connection.authenticationUrl)
        .orElseFail(new Exception("WIT: authenticationUrl missing"))
      clientId     <- ZIO.fromOption(connection.clientId).orElseFail(new Exception("WIT: clientId missing"))
      clientSecret <- ZIO.fromOption(connection.clientSecret).orElseFail(new Exception("WIT: clientSecret missing"))
      user         <- ZIO.fromOption(connection.user).orElseFail(new Exception("WIT: user missing"))
      password     <- ZIO.fromOption(connection.password).orElseFail(new Exception("WIT: password missing"))
      token        <- getOrRefreshToken(connection.id, authUrl, clientId, clientSecret, user, password)
      from = since.getOrElse(Instant.now().minusSeconds(90)).toString
      url  = s"${connection.url}/$meterId?From=${java.net.URLEncoder.encode(from, "UTF-8")}"
      body <- get(url, Map("Authorization" -> s"Bearer $token"))
      result <- ZIO.fromEither(parseWit(body, assetType))
        .mapError(e => new Exception(s"WIT parse: $e"))
    yield result

  private def getOrRefreshToken(
      connectionId: String,
      authUrl: String,
      clientId: String,
      clientSecret: String,
      user: String,
      password: String
  ): Task[String] =
    for
      cached <- tokenCache.get.map(_.get(connectionId))
      token <- cached match
        case Some((t, expiry)) if Instant.now().isBefore(expiry.minusSeconds(60)) => ZIO.succeed(t)
        case _ =>
          for
            resp <- post(
              s"$authUrl/token",
              formBody(
                "grant_type"    -> "password",
                "client_id"     -> clientId,
                "client_secret" -> clientSecret,
                "username"      -> user,
                "password"      -> password
              ),
              Map("Content-Type" -> "application/x-www-form-urlencoded")
            )
            j <- ZIO.fromEither(parse(resp)).mapError(e => new Exception(s"WIT token: $e"))
            t <- ZIO.fromEither(j.hcursor.downField("access_token").as[String]).mapError(new Exception(_))
            expiresIn = j.hcursor.downField("expires_in").as[Long].getOrElse(3600L)
            expiry    = Instant.now().plusSeconds(expiresIn)
            _ <- tokenCache.update(_.updated(connectionId, (t, expiry)))
          yield t
    yield token

  private def parseWit(body: String, assetType: AssetType): Either[String, List[AssetMeasurement]] =
    parse(body).left.map(_.message).flatMap { json =>
      json.as[List[Json]].left.map(_.message).map { datasets =>
        val sign = if assetType == AssetType.Production then -1.0 else 1.0
        datasets.flatMap { ds =>
          for
            tsStr <- ds.hcursor.downField("T").as[String].toOption
            ts = scala.util.Try(Instant.parse(tsStr)).toOption.getOrElse(Instant.now())
            kw <- ds.hcursor.downField("V").as[Double].toOption
          yield AssetMeasurement(
            timestamp = ts,
            instantWatts = kw * 1000 * sign,
            instantWattsL1 = None,
            instantWattsL2 = None,
            instantWattsL3 = None,
            instantAmps = None,
            instantAmpsL1 = None,
            instantAmpsL2 = None,
            instantAmpsL3 = None,
            instantVolts = None,
            instantVoltsL1 = None,
            instantVoltsL2 = None,
            instantVoltsL3 = None,
            consumptionWh = 0.0, // derived from power × interval at consumption task level
            stateOfCharge = None
          )
        }
      }
    }

// ── Registry ─────────────────────────────────────────────────────────────────

/** Maps connector types to connector instances. */
class AssetConnectorRegistry(connectors: Map[AssetConnectorType, AssetConnector]):
  def get(connectorType: AssetConnectorType): Option[AssetConnector] = connectors.get(connectorType)

object AssetConnectorRegistry:

  val live: ZLayer[Any, Nothing, AssetConnectorRegistry] =
    ZLayer.fromZIO {
      for
        ioThinkCache <- Ref.make(Map.empty[String, (String, Instant)])
        witCache     <- Ref.make(Map.empty[String, (String, Instant)])
      yield new AssetConnectorRegistry(Map(
        AssetConnectorType.Greencom  -> new GreencomConnector,
        AssetConnectorType.Schneider -> new SchneiderConnector,
        AssetConnectorType.IOThink   -> new IOThinkConnector(ioThinkCache),
        AssetConnectorType.Lacroix   -> new LacroixConnector,
        AssetConnectorType.Wit       -> new WitConnector(witCache)
      ))
    }
