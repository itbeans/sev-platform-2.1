package io.itbeans.ev.notification

import io.circe._
import io.circe.generic.semiauto._

// ---------------------------------------------------------------------------
// NotificationRequest — Kafka message payload for the notifications.outbound topic.
//
// `data` is a flat Map[String, String] carrying notification-type-specific
// fields, mirroring the TypeScript `sourceData: any` pattern without requiring
// a separate case class per notification type (40+).
//
// Producers (TypeScript OCPP Processor / future Scala services) serialise this
// to JSON; the ev-notification Kafka consumer deserialises and dispatches it to
// the appropriate channel(s).
// ---------------------------------------------------------------------------

case class NotificationRequest(
    notificationType: NotificationType,
    tenantId: String,
    tenantName: String,
    tenantSubdomain: String,
    tenantLogoUrl: Option[String],
    userId: Option[String],
    userEmail: Option[String],
    userName: Option[String],
    userLocale: String,
    mobileToken: Option[String],
    severity: NotificationSeverity,
    channels: Set[NotificationChannel],
    data: Map[String, String],
    /** Stable key used for deduplication: hash of (notificationType + stationId + userId). */
    sourceId: String,
    /** Human-readable description for the dedup store (mirrors TypeScript sourceDescr). */
    sourceDescr: String
)

object NotificationRequest:

  given Decoder[NotificationType] = Decoder.decodeString.emap { s =>
    scala.util.Try(NotificationType.valueOf(s)).toEither.left.map(_.getMessage)
  }
  given Encoder[NotificationType] = Encoder.encodeString.contramap(_.toString)

  given Decoder[NotificationSeverity] = Decoder.decodeString.emap { s =>
    scala.util.Try(NotificationSeverity.valueOf(s)).toEither.left.map(_.getMessage)
  }
  given Encoder[NotificationSeverity] = Encoder.encodeString.contramap(_.toString)

  given Decoder[NotificationChannel] = Decoder.decodeString.emap { s =>
    scala.util.Try(NotificationChannel.valueOf(s)).toEither.left.map(_.getMessage)
  }
  given Encoder[NotificationChannel] = Encoder.encodeString.contramap(_.toString)

  given Decoder[NotificationRequest] = deriveDecoder[NotificationRequest]
  given Encoder[NotificationRequest] = deriveEncoder[NotificationRequest]
