package io.itbeans.ev.notification

import io.circe.parser.decode
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.notification.NotificationRequest.given
import io.itbeans.ev.notification.email.{EmailNotificationTask, EmailTemplates}
import io.itbeans.ev.notification.fcm.FcmNotificationTask
import zio._
import zio.test._
import zio.test.Assertion._

object NotificationServiceSpec extends ZIOSpecDefault:

  private val baseRequest = NotificationRequest(
    notificationType = NotificationType.EndOfCharge,
    tenantId = "test-tenant",
    tenantName = "Test Tenant",
    tenantSubdomain = "test",
    tenantLogoUrl = None,
    userId = Some("user-1"),
    userEmail = Some("user@example.com"),
    userName = Some("Alice"),
    userLocale = "en",
    mobileToken = None,
    severity = NotificationSeverity.Info,
    channels = Set(NotificationChannel.Email),
    data = Map(
      "chargeBoxID"      -> "STATION-001",
      "totalConsumption" -> "12.5",
      "totalDuration"    -> "2h 30m",
      "totalInactivity"  -> "0m"
    ),
    sourceId = "end-of-charge-tx-123",
    sourceDescr = "EndOfCharge"
  )

  override def spec = suite("ev-notification")(
    suite("NotificationService")(
      test("suppresses email when already sent within rate-limit window") {
        val storage = new NotificationStorage:
          def hasAlreadySent(t: TenantId, ch: NotificationChannel, src: String, mins: Int) =
            ZIO.succeed(true)
          def markSent(t: TenantId, ch: NotificationChannel, src: String, nt: NotificationType) =
            ZIO.unit

        val email = new EmailNotificationTask:
          def send(r: NotificationRequest) = ZIO.die(new Exception("Should not send"))

        val fcm = new FcmNotificationTask:
          def send(r: NotificationRequest) = ZIO.unit

        val svc = new DefaultNotificationService(email, fcm, storage)
        svc.dispatch(baseRequest).as(assertCompletes)
      },
      test("calls email channel when not rate-limited") {
        for
          sentRef <- Ref.make(false)
          storage = new NotificationStorage:
            def hasAlreadySent(t: TenantId, ch: NotificationChannel, src: String, mins: Int) =
              ZIO.succeed(false)
            def markSent(t: TenantId, ch: NotificationChannel, src: String, nt: NotificationType) =
              ZIO.unit
          email = new EmailNotificationTask:
            def send(r: NotificationRequest) = sentRef.set(true)
          fcm = new FcmNotificationTask:
            def send(r: NotificationRequest) = ZIO.unit
          svc = new DefaultNotificationService(email, fcm, storage)
          _    <- svc.dispatch(baseRequest)
          sent <- sentRef.get
        yield assertTrue(sent)
      },
      test("continues if one channel fails — marks it as sent to avoid retry storms") {
        for
          markedRef <- Ref.make(false)
          storage = new NotificationStorage:
            def hasAlreadySent(t: TenantId, ch: NotificationChannel, src: String, mins: Int) =
              ZIO.succeed(false)
            def markSent(t: TenantId, ch: NotificationChannel, src: String, nt: NotificationType) =
              markedRef.set(true)
          email = new EmailNotificationTask:
            def send(r: NotificationRequest) = ZIO.fail(new Exception("SMTP down"))
          fcm = new FcmNotificationTask:
            def send(r: NotificationRequest) = ZIO.unit
          svc = new DefaultNotificationService(email, fcm, storage)
          _      <- svc.dispatch(baseRequest) // must not propagate the email error
          marked <- markedRef.get
        yield assertTrue(marked)
      }
    ),
    suite("EmailTemplates")(
      test("resolve returns content for EndOfCharge") {
        val result = EmailTemplates.resolve(baseRequest)
        assertTrue(result.isDefined) &&
        assertTrue(result.get.subject.contains("12.5 kWh")) &&
        assertTrue(result.get.htmlBody.contains("STATION-001")) &&
        assertTrue(result.get.htmlBody.contains("Alice")) &&
        assertTrue(result.get.to == "user@example.com")
      },
      test("resolve returns content for BillingNewInvoice") {
        val req = baseRequest.copy(
          notificationType = NotificationType.BillingNewInvoice,
          data = Map(
            "invoiceNumber" -> "INV-2024-001",
            "invoiceAmount" -> "35.90",
            "currency"      -> "EUR",
            "invoiceStatus" -> "unpaid",
            "invoiceUrl"    -> "https://example.com/invoices/1"
          )
        )
        val result = EmailTemplates.resolve(req)
        assertTrue(result.isDefined) &&
        assertTrue(result.get.subject.contains("INV-2024-001")) &&
        assertTrue(result.get.htmlBody.contains("35.90"))
      },
      test("resolve returns None for not-yet-implemented type") {
        val req = baseRequest.copy(notificationType = NotificationType.CarsSynchronizationFailed)
        assertTrue(EmailTemplates.resolve(req).isEmpty)
      },
      test("htmlBody is valid HTML with tenant branding") {
        val result = EmailTemplates.resolve(baseRequest).get
        assertTrue(result.htmlBody.contains("<!DOCTYPE html>")) &&
        assertTrue(result.htmlBody.contains("Test Tenant")) &&
        assertTrue(result.htmlBody.contains("#00376C")) // Info severity brand colour
      }
    ),
    suite("NotificationRequest codec")(
      test("roundtrips through JSON") {
        val json    = baseRequest.asJson.noSpaces
        val decoded = decode[NotificationRequest](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.get == baseRequest)
      },
      test("decodes NotificationType from string") {
        val json = baseRequest.copy(
          notificationType = NotificationType.BillingNewInvoice
        ).asJson.noSpaces
        val decoded = decode[NotificationRequest](json)
        assertTrue(decoded.isRight) &&
        assertTrue(decoded.toOption.get.notificationType == NotificationType.BillingNewInvoice)
      },
      test("decodes both channels") {
        val req = baseRequest.copy(channels =
          Set(
            NotificationChannel.Email,
            NotificationChannel.RemotePushNotification
          )
        )
        val json = req.asJson.noSpaces
        val got  = decode[NotificationRequest](json)
        assertTrue(got.isRight) &&
        assertTrue(got.toOption.get.channels == Set(
          NotificationChannel.Email,
          NotificationChannel.RemotePushNotification
        ))
      }
    )
  )
