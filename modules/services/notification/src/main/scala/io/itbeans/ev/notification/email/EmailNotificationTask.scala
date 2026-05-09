package io.itbeans.ev.notification.email

import io.itbeans.ev.notification.{NotificationConfig, NotificationRequest}
import jakarta.mail._
import jakarta.mail.internet._
import zio._

import java.util.Properties

// ---------------------------------------------------------------------------
// EmailNotificationTask — SMTP email delivery via Jakarta Mail.
//
// All SMTP I/O is wrapped in ZIO.attemptBlocking so the ZIO runtime does not
// block its thread pool.  The Jakarta Mail Session is created once and reused
// across sends (Session objects are thread-safe).
//
// Replaces: EMailNotificationTask.ts + EmailMjmlTemplate.ts
// ---------------------------------------------------------------------------

trait EmailNotificationTask:
  def send(req: NotificationRequest): Task[Unit]

final class SmtpEmailNotificationTask(config: NotificationConfig) extends EmailNotificationTask:

  private val session: Session =
    val props = new Properties()
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", config.smtp.tls.toString)
    props.put("mail.smtp.host", config.smtp.host)
    props.put("mail.smtp.port", config.smtp.port.toString)
    props.put("mail.smtp.ssl.trust", config.smtp.host)
    Session.getInstance(
      props,
      new Authenticator:
        override protected def getPasswordAuthentication: PasswordAuthentication =
          new PasswordAuthentication(config.smtp.user, config.smtp.password)
    )

  override def send(req: NotificationRequest): Task[Unit] =
    EmailTemplates.resolve(req) match
      case None =>
        ZIO.logDebug(s"No email template for ${req.notificationType} — skipping")
      case Some(content) if content.to.isEmpty =>
        ZIO.logWarning(s"No recipient email for ${req.notificationType} userId=${req.userId}")
      case Some(content) =>
        ZIO
          .attemptBlocking {
            val message = new MimeMessage(session)
            message.setFrom(new InternetAddress(config.smtp.from))
            message.setRecipients(
              Message.RecipientType.TO,
              InternetAddress.parse(content.to).asInstanceOf[Array[jakarta.mail.Address]]
            )
            message.setSubject(content.subject, "UTF-8")
            message.setContent(content.htmlBody, "text/html; charset=utf-8")
            Transport.send(message)
          }
          .tapError(err =>
            ZIO.logError(
              s"SMTP send failed to=${content.to} type=${req.notificationType}: ${err.getMessage}"
            )
          )
          .unit

object SmtpEmailNotificationTask:

  val live: ZLayer[NotificationConfig, Nothing, EmailNotificationTask] =
    ZLayer.fromFunction(new SmtpEmailNotificationTask(_))
