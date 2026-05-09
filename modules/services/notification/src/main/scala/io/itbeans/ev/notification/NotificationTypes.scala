package io.itbeans.ev.notification

// ---------------------------------------------------------------------------
// Notification domain types.
//
// These mirror the TypeScript enums in UserNotifications.ts.
// Severity colours match the TypeScript NotificationSeverity enum so that
// the mobile app receives the same colour codes as before.
// ---------------------------------------------------------------------------

enum NotificationType:
  case EndOfCharge
  case OptimalChargeReached
  case EndOfSession
  case EndOfSignedSession
  case TransactionStarted
  case SessionNotStarted
  case PreparingSessionNotStarted
  case UserAccountStatusChanged
  case NewRegisteredUser
  case UserAccountInactivity
  case VerificationEmail
  case AdminAccountVerification
  case AccountVerificationNotification
  case ChargingStationStatusError
  case ChargingStationRegistered
  case UnknownUserBadged
  case OfflineChargingStations
  case BillingNewInvoice
  case BillingSynchronizationFailed
  case BillingAccountCreationLink
  case BillingAccountActivation
  case OcpiPatchStatusError
  case OicpPatchStatusError
  case ComputeChargingProfilesFailed
  case CarsSynchronizationFailed
  case EndUserError

/** Matches TypeScript NotificationSeverity colour codes used by the mobile app. */
enum NotificationSeverity(val color: String):
  case Info    extends NotificationSeverity("#00376C")
  case Warning extends NotificationSeverity("#FB8C00")
  case Error   extends NotificationSeverity("#ee0000")

enum NotificationChannel:
  case Email
  case RemotePushNotification
