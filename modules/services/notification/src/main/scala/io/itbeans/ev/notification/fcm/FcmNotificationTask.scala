package io.itbeans.ev.notification.fcm

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.itbeans.ev.notification.{NotificationConfig, NotificationRequest, NotificationType}
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import zio._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.http.HttpRequest.BodyPublishers
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec
import java.time.Instant
import java.util.Base64

// ---------------------------------------------------------------------------
// FcmNotificationTask — Firebase Cloud Messaging HTTP v1 REST API.
//
// Auth flow (service account JWT Bearer):
//   1. Parse client_email + private_key from serviceAccountJson
//   2. Sign a short-lived JWT (RS256) scoped to firebase.messaging
//   3. Exchange at oauth2.googleapis.com/token for a Bearer access token
//   4. POST to fcm.googleapis.com/v1/projects/{projectId}/messages:send
//
// The access token is cached in a ZIO Ref and refreshed 60 s before expiry.
// All HTTP calls use java.net.http.HttpClient (Java 11+) wrapped in
// ZIO.attemptBlocking.
//
// When fcm.enabled = false, all sends are no-ops (local / CI environments).
//
// Replaces: RemotePushNotificationTask.ts
// ---------------------------------------------------------------------------

trait FcmNotificationTask:
  def send(req: NotificationRequest): Task[Unit]

/** No-op implementation used when FCM is disabled in config. */
object NoOpFcmNotificationTask extends FcmNotificationTask:

  override def send(req: NotificationRequest): Task[Unit] =
    ZIO.logDebug(s"FCM disabled — skipping push for ${req.notificationType}")

final class HttpFcmNotificationTask(
    config: FcmConfig,
    http: HttpClient,
    tokenRef: Ref[(String, Instant)]
) extends FcmNotificationTask:

  private val tokenUrl = "https://oauth2.googleapis.com/token"
  private val fcmUrl   = s"https://fcm.googleapis.com/v1/projects/${config.projectId}/messages:send"
  private val scope    = "https://www.googleapis.com/auth/firebase.messaging"

  override def send(req: NotificationRequest): Task[Unit] =
    req.mobileToken match
      case None =>
        ZIO.logDebug(s"No mobile token for user=${req.userId} — skipping FCM")
      case Some(deviceToken) =>
        for
          token <- getOrRefreshToken
          payload = buildPayload(req, deviceToken).noSpaces
          _ <- postJson(fcmUrl, payload, Some(token))
            .tapError(err =>
              ZIO.logError(s"FCM send failed type=${req.notificationType}: ${err.getMessage}")
            )
            .unit
        yield ()

  private def getOrRefreshToken: Task[String] =
    for
      pair <- tokenRef.get
      (token, expiresAt) = pair
      result <-
        if Instant.now().isBefore(expiresAt.minusSeconds(60))
        then ZIO.succeed(token)
        else refreshToken
    yield result

  private def refreshToken: Task[String] =
    for
      sa <- parseServiceAccount
      now = Instant.now()
      claim = JwtClaim(
        content = Json.obj(
          "iss"   -> sa.clientEmail.asJson,
          "scope" -> scope.asJson,
          "aud"   -> tokenUrl.asJson,
          "iat"   -> now.getEpochSecond.asJson,
          "exp"   -> (now.getEpochSecond + 3600).asJson
        ).noSpaces
      )
      privateKey <- buildPrivateKey(sa.privateKey)
      jwt  = JwtCirce.encode(claim, privateKey, JwtAlgorithm.RS256)
      body = s"grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=$jwt"
      respJson <- postFormEncoded(tokenUrl, body)
      token <- ZIO
        .fromEither(respJson.hcursor.downField("access_token").as[String])
        .mapError(e => new Exception(s"Missing access_token in OAuth2 response: $e"))
      _ <- tokenRef.set((token, now.plusSeconds(3500)))
    yield token

  private def parseServiceAccount: Task[ServiceAccount] =
    ZIO
      .fromEither(
        parse(config.serviceAccountJson)
          .flatMap(_.as[ServiceAccount])
      )
      .mapError(e => new Exception(s"Failed to parse FCM service account JSON: $e"))

  private def buildPrivateKey(pem: String): Task[RSAPrivateKey] =
    ZIO.attemptBlocking {
      val stripped = pem
        .replace("-----BEGIN PRIVATE KEY-----", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replaceAll("\\s+", "")
      val keyBytes = Base64.getDecoder.decode(stripped)
      KeyFactory
        .getInstance("RSA")
        .generatePrivate(new PKCS8EncodedKeySpec(keyBytes))
        .asInstanceOf[RSAPrivateKey]
    }

  private def postFormEncoded(url: String, body: String): Task[Json] =
    ZIO
      .attemptBlocking {
        val req = HttpRequest
          .newBuilder(URI.create(url))
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(BodyPublishers.ofString(body))
          .build()
        http.send(req, HttpResponse.BodyHandlers.ofString()).body()
      }
      .flatMap(raw => ZIO.fromEither(parse(raw)).mapError(e => new Exception(e.getMessage)))

  private def postJson(url: String, body: String, bearer: Option[String]): Task[String] =
    ZIO.attemptBlocking {
      // HttpRequest.Builder is immutable — .header() returns a new builder each time
      val base = HttpRequest
        .newBuilder(URI.create(url))
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString(body))
      val withAuth = bearer.fold(base)(t => base.header("Authorization", s"Bearer $t"))
      http.send(withAuth.build(), HttpResponse.BodyHandlers.ofString()).body()
    }

  private def buildPayload(req: NotificationRequest, deviceToken: String): Json =
    val (title, body) = notificationText(req)
    Json.obj(
      "message" -> Json.obj(
        "token" -> deviceToken.asJson,
        "notification" -> Json.obj(
          "title" -> title.asJson,
          "body"  -> body.asJson
        ),
        "android" -> Json.obj(
          "priority" -> "high".asJson,
          "notification" -> Json.obj(
            "channel_id" -> "e-Mobility".asJson,
            "color"      -> req.severity.color.asJson
          )
        ),
        "data" -> Json.obj(
          "tenantID"         -> req.tenantId.asJson,
          "tenantSubdomain"  -> req.tenantSubdomain.asJson,
          "notificationType" -> req.notificationType.toString.asJson
        )
      )
    )

  private def notificationText(req: NotificationRequest): (String, String) =
    val d = req.data
    req.notificationType match
      case NotificationType.EndOfCharge =>
        ("Charging complete", s"Session ended. ${d.getOrElse("totalConsumption", "")} kWh delivered.")
      case NotificationType.OptimalChargeReached =>
        ("Optimal charge reached", s"Your vehicle is at ${d.getOrElse("stateOfCharge", "")}% charge.")
      case NotificationType.TransactionStarted =>
        ("Charging started", s"Session started at ${d.getOrElse("chargeBoxID", "")}.")
      case NotificationType.BillingNewInvoice =>
        (
          "New invoice",
          s"Invoice ${d.getOrElse("invoiceNumber", "")} — " +
            s"${d.getOrElse("invoiceAmount", "")} ${d.getOrElse("currency", "")}"
        )
      case NotificationType.ChargingStationStatusError =>
        ("Station error", s"${d.getOrElse("chargeBoxID", "")}: ${d.getOrElse("errorCode", "unknown error")}")
      case NotificationType.UnknownUserBadged =>
        ("Unknown badge", s"Unknown RFID badge presented at ${d.getOrElse("chargeBoxID", "")}.")
      case _ =>
        (req.notificationType.toString, d.getOrElse("message", "You have a new notification."))

/** Config subset needed by HttpFcmNotificationTask (avoids coupling to full NotificationConfig). */
private case class FcmConfig(projectId: String, serviceAccountJson: String)

private case class ServiceAccount(clientEmail: String, privateKey: String)

private object ServiceAccount:

  given Decoder[ServiceAccount] = Decoder.instance { c =>
    for
      email <- c.downField("client_email").as[String]
      key   <- c.downField("private_key").as[String]
    yield ServiceAccount(email, key)
  }

object FcmNotificationTaskLive:

  val live: ZLayer[NotificationConfig, Nothing, FcmNotificationTask] =
    ZLayer.fromZIO {
      for
        cfg      <- ZIO.service[NotificationConfig]
        tokenRef <- Ref.make(("", Instant.EPOCH))
      yield
        if cfg.fcm.enabled
        then
          val http = HttpClient.newBuilder().build()
          new HttpFcmNotificationTask(
            FcmConfig(cfg.fcm.projectId, cfg.fcm.serviceAccountJson),
            http,
            tokenRef
          )
        else NoOpFcmNotificationTask
    }
