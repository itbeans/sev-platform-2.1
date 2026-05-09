package io.itbeans.ev.notification

import zio.Config

// ---------------------------------------------------------------------------
// NotificationConfig — ZIO Config description for SMTP and FCM settings.
//
// HOCON layout (application.conf):
//   notification {
//     smtp { host, port, user, password, from, tls }
//     fcm  { projectId, serviceAccountJson, enabled }
//   }
// ---------------------------------------------------------------------------

case class SmtpConfig(
    host: String,
    port: Int,
    user: String,
    password: String,
    from: String,
    tls: Boolean
)

case class FcmConfig(
    projectId: String,
    serviceAccountJson: String,
    /** Set to false in local / test environments to suppress FCM calls. */
    enabled: Boolean
)

case class NotificationConfig(
    smtp: SmtpConfig,
    fcm: FcmConfig
)

object NotificationConfig:

  private val smtpConfig: Config[SmtpConfig] = (
    Config.string("host") zip Config.int("port") zip Config.string("user") zip
      Config.string("password") zip Config.string("from") zip Config.boolean("tls")
  ).map { case (host, port, user, pass, from, tls) => SmtpConfig(host, port, user, pass, from, tls) }

  private val fcmConfig: Config[FcmConfig] = (
    Config.string("projectId") zip Config.string("serviceAccountJson") zip Config.boolean("enabled")
  ).map { case (pid, sa, en) => FcmConfig(pid, sa, en) }

  given Config[NotificationConfig] = (
    smtpConfig.nested("smtp") zip fcmConfig.nested("fcm")
  ).nested("notification").map { case (smtp, fcm) => NotificationConfig(smtp, fcm) }
