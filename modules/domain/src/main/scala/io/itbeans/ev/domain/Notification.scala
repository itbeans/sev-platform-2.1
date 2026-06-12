package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema, StandardType}
import java.time.Instant

opaque type NotificationId = String

object NotificationId:
  def apply(v: String): NotificationId             = v
  def unapply(id: NotificationId): Some[String]    = Some(id)
  extension (id: NotificationId) def value: String = id

  given Schema[NotificationId] =
    Schema.Primitive(StandardType.StringType, zio.Chunk.empty).transform(NotificationId(_), _.value)

// ---------------------------------------------------------------------------
// Notification domain — outbound email + push notification types.
// Mirrors the TypeScript NotificationHandler notification type catalogue.
// ---------------------------------------------------------------------------

enum NotificationType:

  case EndOfCharge,
    OptimalChargeReached,
    EndOfSession,
    EndOfSignedSession,
    TransactionStarted,
    SessionNotStarted,
    PreparingSessionNotStarted,
    UserAccountStatusChanged,
    NewRegisteredUser,
    UserAccountInactivity,
    VerificationEmail,
    AdminAccountVerification,
    AccountVerificationNotification,
    ChargingStationStatusError,
    ChargingStationRegistered,
    UnknownUserBadged,
    OfflineChargingStations,
    BillingNewInvoice,
    BillingSynchronizationFailed,
    BillingAccountCreationLink,
    BillingAccountActivation,
    OcpiPatchStatusError,
    OicpPatchStatusError,
    ComputeChargingProfilesFailed,
    CarsSynchronizationFailed,
    EndUserError

enum NotificationSeverity(val color: String):
  case Info    extends NotificationSeverity("#00376C")
  case Warning extends NotificationSeverity("#FB8C00")
  case Error   extends NotificationSeverity("#ee0000")

enum NotificationChannel:
  case Email, RemotePushNotification

// Sent notification record stored in MongoDB (user-visible history)
case class Notification(
    id: NotificationId,
    tenantId: TenantId,
    userId: UserId,
    channel: NotificationChannel,
    notificationType: NotificationType,
    severity: NotificationSeverity,
    sourceId: String, // transaction ID, station ID, etc.
    sourceDescr: String,
    chargeBoxId: Option[ChargingStationId],
    timestamp: Instant,
    data: Map[String, String] // template-variable payload
)

// Request published to the notifications.outbound Kafka topic
case class NotificationRequest(
    notificationType: NotificationType,
    tenantId: TenantId,
    tenantName: String,
    tenantSubdomain: String,
    tenantLogoUrl: Option[String],
    userId: Option[UserId],
    userEmail: Option[String],
    userName: Option[String],
    userLocale: String,
    mobileToken: Option[String],
    severity: NotificationSeverity,
    channels: Set[NotificationChannel],
    data: Map[String, String],
    sourceId: String,
    sourceDescr: String
)

// Per-user notification preferences (which channels are enabled per type)
case class NotificationPreferences(
    userId: UserId,
    tenantId: TenantId,
    preferences: Map[String, Set[NotificationChannel]] // notificationType.name → channels
)

object Notification:
  given Schema[NotificationType] = DeriveSchema.gen
  given Schema[NotificationSeverity] = DeriveSchema.gen
  given Schema[NotificationChannel] = DeriveSchema.gen
  given Schema[Notification] = DeriveSchema.gen
  given Schema[NotificationRequest] = DeriveSchema.gen
  given Schema[NotificationPreferences] = DeriveSchema.gen
