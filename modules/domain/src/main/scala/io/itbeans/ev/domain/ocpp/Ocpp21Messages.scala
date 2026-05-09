package io.itbeans.ev.domain.ocpp

import io.circe.Json
import io.itbeans.ev.domain.TransactionStop
import io.itbeans.ev.domain.Transaction.given
import zio.schema.{DeriveSchema, Schema}

// ---------------------------------------------------------------------------
// OCPP 2.1 Message Framing
//
// OCPP 2.1 defines 5 message types (vs. 3 in OCPP 1.6):
//   2 = CALL              (request expecting response)
//   3 = CALLRESULT        (response to CALL)
//   4 = CALLERROR         (error response to CALL)
//   5 = CALLRESULTERROR   (NEW in 2.1: error in response to a CALLRESULT)
//   6 = SEND              (NEW in 2.1: one-way, no response expected)
// ---------------------------------------------------------------------------

enum OcppMessageType(val id: Int):
  case Call            extends OcppMessageType(2)
  case CallResult      extends OcppMessageType(3)
  case CallError       extends OcppMessageType(4)
  case CallResultError extends OcppMessageType(5) // OCPP 2.1 new
  case Send            extends OcppMessageType(6) // OCPP 2.1 new (one-way)

// ---------------------------------------------------------------------------
// OCPP 2.1 Action names
// Inherits all OCPP 2.0.1 actions plus new 2.1 additions.
// ---------------------------------------------------------------------------

enum OcppAction:
  // ── A: Security ──────────────────────────────────────────────────────────
  case SecurityEventNotification
  case LogStatusNotification
  // ── B: Provisioning ──────────────────────────────────────────────────────
  case BootNotification
  case Heartbeat
  case GetBaseReport
  case GetReport
  case NotifyReport
  case SetNetworkProfile
  case GetNetworkProfile
  case Reset
  // ── C: Authorization ─────────────────────────────────────────────────────
  case Authorize
  case ClearCache
  case SendLocalList
  case GetLocalListVersion
  // ── D: LocalAuthorizationList (covered by C above) ───────────────────────
  // ── E: Transactions ──────────────────────────────────────────────────────
  case TransactionEvent // Replaces OCPP 1.6 Start/StopTransaction + MeterValues
  case MeterValues      // Still present in 2.1 for backward compat paths
  case RequestStartTransaction
  case RequestStopTransaction
  case GetTransactionStatus
  // ── F: RemoteControl ─────────────────────────────────────────────────────
  case TriggerMessage
  case UpdateFirmware
  case CancelReservation
  case ReserveNow
  case UnlockConnector
  case ChangeAvailability
  // ── G: Availability ──────────────────────────────────────────────────────
  case StatusNotification
  case NotifyEVChargingNeeds
  // ── H: Reservation ───────────────────────────────────────────────────────
  // (CancelReservation, ReserveNow above)
  // ── I: TariffAndCost ─────────────────────────────────────────────────────
  case CostUpdated
  case SetDisplayMessage
  case ClearDisplayMessage
  case GetDisplayMessages
  // ── J: MeterValues ───────────────────────────────────────────────────────
  // (MeterValues, TransactionEvent cover this)
  // ── K: SmartCharging ─────────────────────────────────────────────────────
  case SetChargingProfile
  case GetChargingProfiles
  case ClearChargingProfile
  case GetCompositeSchedule
  case NotifyChargingLimit
  case NotifyEVChargingSchedule
  case ReportChargingProfiles
  // ── L: FirmwareManagement ────────────────────────────────────────────────
  case PublishFirmware
  case PublishFirmwareStatusNotification
  case UnpublishFirmware
  case FirmwareStatusNotification
  case GetLog
  case LogStatusNotificationSecure
  // ── M: ISO 15118 ─────────────────────────────────────────────────────────
  case Get15118EVCertificate
  case GetCertificateStatus
  case InstallCertificate
  case DeleteCertificate
  case GetInstalledCertificateIds
  case CertificateSigned
  // ── N: DisplayMessage ────────────────────────────────────────────────────
  // (covered in I above)
  // ── O: DataTransfer ──────────────────────────────────────────────────────
  case DataTransfer
  // ── P: Diagnostics ───────────────────────────────────────────────────────
  case GetVariables
  case SetVariables
  case SetMonitoringBase
  case SetMonitoringLevel
  case SetVariableMonitoring
  case GetMonitoringReport
  case ClearVariableMonitoring
  case NotifyMonitoringReport
  case NotifyEvent
  case ClearChargingLimit
  // ─────────────────────────────────────────────────────────────────────────
  // OCPP 2.1 NEW ACTIONS
  // ─────────────────────────────────────────────────────────────────────────
  // ── Q: BidirectionalCharging (V2X) ───────────────────────────────────────
  case NotifyDERStartStop        // Station notifies CSMS of DER start/stop
  case NotifyDERAlarm            // Station reports DER alarm condition
  case GetDERControl             // CSMS queries current DER control settings
  case SetDERControl             // CSMS sends DER control settings to station
  case NotifyPriorityCharging    // Station notifies priority charging state change
  case PullDynamicScheduleUpdate // Station pulls schedule update
  case UpdateDynamicSchedule     // CSMS pushes dynamic schedule update
  case UsePriorityCharging       // CSMS requests priority charging mode
  // ── R: DERControl ────────────────────────────────────────────────────────
  case AFRRSignal // Automatic Frequency Restoration Reserve signal
  // ── S: BatterySwap ───────────────────────────────────────────────────────
  case BatterySwap          // Battery swap event notification
  case GetBatterySwapReport // CSMS requests battery swap report
  // ── Additional 2.1 enhancements ──────────────────────────────────────────
  case AdjustPeriodicEventStream // Control periodic event streams
  case ClosePeriodicEventStream
  case GetPeriodicEventStream
  case OpenPeriodicEventStream
  case NotifyPeriodicEventStream
  case SetDefaultTariff // OCPP 2.1 local cost calculation
  case GetDefaultTariff

object OcppAction:
  given Schema[OcppAction] = DeriveSchema.gen

// ---------------------------------------------------------------------------
// OCPP 2.1 IdToken — replaces the simple RFID string of OCPP 1.6
// ---------------------------------------------------------------------------

case class IdToken(
    idToken: String,
    tokenType: IdTokenType,
    additionalInfo: List[AdditionalInfo] = Nil
)

enum IdTokenType:

  case Central, eMAID, EVCCID, ISO14443, ISO15693, KeyCode, Local,
    MacAddress, NoAuthorization, NEMA, eID, RFID

case class AdditionalInfo(additionalIdToken: String, tokenType: String)

// ---------------------------------------------------------------------------
// OCPP 2.1 EVSE — replaces connector model of OCPP 1.6
// ---------------------------------------------------------------------------

case class EVSE(id: Int, connectorId: Option[Int])

// ---------------------------------------------------------------------------
// OCPP 2.1 BootNotification
// ---------------------------------------------------------------------------

case class ChargingStationInfo(
    model: String,
    vendorName: String,
    serialNumber: Option[String],
    firmwareVersion: Option[String],
    modem: Option[Modem]
)

case class Modem(iccid: Option[String], imsi: Option[String])

case class BootNotificationRequest(
    chargingStation: ChargingStationInfo,
    reason: BootReason
)

enum BootReason:

  case ApplicationReset, FirmwareUpdate, LocalReset, PowerUp,
    RemoteReset, ScheduledReset, Triggered, Unknown, Watchdog

case class BootNotificationResponse(
    currentTime: java.time.Instant,
    interval: Int,
    status: RegistrationStatus,
    statusInfo: Option[StatusInfo] = None
)

enum RegistrationStatus:
  case Accepted, Pending, Rejected

// ---------------------------------------------------------------------------
// OCPP 2.1 TransactionEvent (replaces Start/StopTransaction + MeterValues)
// ---------------------------------------------------------------------------

enum TransactionEventType:
  case Started, Updated, Ended

enum TriggerReason:

  case Authorized, CablePluggedIn, ChargingRateChanged, ChargingStateChanged,
    Deauthorized, EnergyLimitReached, EVCommunicationLost, EVConnectTimeout,
    MeterValueClock, MeterValuePeriodic, Other, RemoteStart, RemoteStop,
    ReservationStart, ReservationEnd, SignedDataReceived, StopAuthorized,
    TimeLimitReached, Trigger, UnlockCommand, StopCharging, EVDeparted,
    EVDetected, Reauthorized, ResumeCharging, SuspendedEV, SuspendedEVSE

case class TransactionEventRequest(
    eventType: TransactionEventType,
    timestamp: java.time.Instant,
    triggerReason: TriggerReason,
    seqNo: Int,
    offline: Boolean,
    numberOfPhasesUsed: Option[Int],
    cableMaxCurrent: Option[Double],
    reservationId: Option[Int],
    transactionInfo: TransactionInfo,
    idToken: Option[IdToken],
    evse: Option[EVSE],
    meterValue: List[MeterValue]
)

case class TransactionInfo(
    transactionId: String,
    chargingState: Option[ChargingState],
    timeSpentCharging: Option[Long],
    stoppedReason: Option[TransactionStop],
    remoteStartId: Option[Int]
)

enum ChargingState:
  case Charging, EVConnected, SuspendedEV, SuspendedEVSE, Idle

// ---------------------------------------------------------------------------
// OCPP 2.1 MeterValue
// ---------------------------------------------------------------------------

case class MeterValue(
    timestamp: java.time.Instant,
    sampledValue: List[SampledValue]
)

case class SampledValue(
    value: Double,
    context: Option[ReadingContext],
    measurand: Option[Measurand],
    phase: Option[Phase],
    location: Option[MeasurementLocation],
    unitOfMeasure: Option[UnitOfMeasure],
    signedMeterValue: Option[SignedMeterValue]
)

case class UnitOfMeasure(unit: String, multiplier: Int = 0)
case class SignedMeterValue(signedMeterData: String, signingMethod: String, encodingMethod: String, publicKey: String)

enum ReadingContext:

  case InterruptionBegin, InterruptionEnd, Other, SampleClock, SamplePeriodic,
    TransactionBegin, TransactionEnd, Trigger

enum Measurand:

  case CurrentExport, CurrentImport, CurrentOffered,
    EnergyActiveExportRegister, EnergyActiveImportRegister,
    EnergyReactiveExportRegister, EnergyReactiveImportRegister,
    EnergyActiveExportInterval, EnergyActiveImportInterval,
    EnergyReactiveExportInterval, EnergyReactiveImportInterval,
    EnergyApparentNet, EnergyActiveNet, EnergyReactiveNet,
    Frequency, PowerActiveExport, PowerActiveImport, PowerFactor,
    PowerOffered, PowerReactiveExport, PowerReactiveImport, RPM,
    SOC, Temperature, Voltage, Undefined

enum Phase:
  case L1, L2, L3, N, `L1-N`, `L2-N`, `L3-N`, `L1-L2`, `L2-L3`, `L3-L1`

enum MeasurementLocation:
  case Body, Cable, EV, Inlet, Outlet

// ---------------------------------------------------------------------------
// OCPP 2.1 StatusNotification
// ---------------------------------------------------------------------------

case class StatusNotificationRequest(
    timestamp: java.time.Instant,
    connectorStatus: ConnectorStatusOcpp21,
    evseId: Int,
    connectorId: Int
)

enum ConnectorStatusOcpp21:
  case Available, Occupied, Reserved, Unavailable, Faulted

// ---------------------------------------------------------------------------
// OCPP 2.1 Authorize
// ---------------------------------------------------------------------------

case class AuthorizeRequest(
    idToken: IdToken,
    certificate: Option[String],
    iso15118CertificateHashData: List[OCSPRequestData]
)

case class OCSPRequestData(
    hashAlgorithm: HashAlgorithmType,
    issuerNameHash: String,
    issuerKeyHash: String,
    serialNumber: String,
    responderURL: Option[String]
)

enum HashAlgorithmType:
  case SHA256, SHA384, SHA512

case class AuthorizeResponse(
    idTokenInfo: IdTokenInfo,
    certificateStatus: Option[AuthorizeCertificateStatus] = None
)

case class IdTokenInfo(
    status: AuthorizationStatus,
    cacheExpiryDateTime: Option[java.time.Instant],
    chargingPriority: Int = 0,
    language1: Option[String],
    language2: Option[String],
    evseId: List[Int] = Nil,
    groupIdToken: Option[IdToken],
    personalMessage: Option[MessageContent]
)

enum AuthorizationStatus:

  case Accepted, Blocked, ConcurrentTx, Expired, Invalid, NoCredit,
    NotAllowedTypeEVSE, NotAtThisLocation, NotAtThisTime, Unknown

case class MessageContent(format: MessageFormat, language: Option[String], content: String)

enum MessageFormat:
  case ASCII, HTML, URI, UTF8

enum AuthorizeCertificateStatus:

  case Accepted, CertificateExpired, CertificateRevoked, NoCertificateAvailable,
    CertChainError, ContractCancelled

// ---------------------------------------------------------------------------
// OCPP 2.1 NEW: SetDERControl / GetDERControl
// DER = Distributed Energy Resources (solar, battery, grid)
// ---------------------------------------------------------------------------

case class DERControlRequest(
    isDefault: Boolean,
    controlType: DERControlType,
    controlData: DERControlData
)

enum DERControlType:

  case EnterService, FreqDroop, FreqWatt, FixedPFAbsorb, FixedPFInject,
    FixedVar, Gradients, HFMustTrip, HFMayTrip, HVMustTrip, HVMayTrip,
    LFMustTrip, LVMustTrip, LVMayTrip, LVMayTripAlt, PowerMonitoring,
    VoltVar, VoltWatt, WattPF, WattVar, LimitMaxDischarge

// Generic DER control data (each control type has its own shape)
type DERControlData = Map[String, Json]

// ---------------------------------------------------------------------------
// OCPP 2.1 NEW: AFRRSignal (Automatic Frequency Restoration Reserve)
// ---------------------------------------------------------------------------

case class AFRRSignalRequest(
    timestamp: java.time.Instant,
    signal: Long // value in W (positive = charge, negative = discharge)
)

case class AFRRSignalResponse(status: GenericStatus, statusInfo: Option[StatusInfo] = None)

enum GenericStatus:
  case Accepted, Rejected

case class StatusInfo(reasonCode: String, additionalInfo: Option[String] = None)

// ---------------------------------------------------------------------------
// OCPP 2.1 NEW: NotifyDERStartStop
// ---------------------------------------------------------------------------

case class NotifyDERStartStopRequest(
    timestamp: java.time.Instant,
    started: Boolean,
    controlId: Option[Int],
    measurementTime: Option[java.time.Instant],
    supersededIds: List[Int]
)

// ---------------------------------------------------------------------------
// OCPP 2.1 NEW: BatterySwap
// ---------------------------------------------------------------------------

case class BatterySwapRequest(
    eventType: BatterySwapEventType,
    timestamp: java.time.Instant,
    evseId: Int,
    batteryData: Option[BatteryData],
    requestId: Option[Int]
)

enum BatterySwapEventType:
  case BatteryIn, BatteryOut

case class BatteryData(
    batteryId: String,
    soC: Option[Int],
    soH: Option[Int],
    productionDate: Option[String],
    vendorInfo: Option[String]
)

// ---------------------------------------------------------------------------
// OCPP 2.1 TransactionStop (reason enum — Scala-side mirror)
// ---------------------------------------------------------------------------
// Re-uses domain.TransactionStop defined in Transaction.scala

// ---------------------------------------------------------------------------
// Common response
// ---------------------------------------------------------------------------

case class GenericDeviceModelStatusResponse(
    status: GenericDeviceModelStatus,
    statusInfo: Option[StatusInfo] = None
)

enum GenericDeviceModelStatus:
  case Accepted, Rejected, NotSupported, EmptyResultSet

object Ocpp21Messages:
  // Make sure all schemas are available for codec derivation
  given Schema[OcppMessageType] = DeriveSchema.gen
  given Schema[OcppAction] = DeriveSchema.gen
  given Schema[IdTokenType] = DeriveSchema.gen
  given Schema[AdditionalInfo] = DeriveSchema.gen
  given Schema[IdToken] = DeriveSchema.gen
  given Schema[EVSE] = DeriveSchema.gen
  given Schema[BootReason] = DeriveSchema.gen
  given Schema[RegistrationStatus] = DeriveSchema.gen
  given Schema[StatusInfo] = DeriveSchema.gen
  given Schema[ChargingStationInfo] = DeriveSchema.gen
  given Schema[Modem] = DeriveSchema.gen
  given Schema[BootNotificationRequest] = DeriveSchema.gen
  given Schema[BootNotificationResponse] = DeriveSchema.gen
  given Schema[TransactionEventType] = DeriveSchema.gen
  given Schema[TriggerReason] = DeriveSchema.gen
  given Schema[ChargingState] = DeriveSchema.gen
  given Schema[ReadingContext] = DeriveSchema.gen
  given Schema[Measurand] = DeriveSchema.gen
  given Schema[Phase] = DeriveSchema.gen
  given Schema[MeasurementLocation] = DeriveSchema.gen
  given Schema[UnitOfMeasure] = DeriveSchema.gen
  given Schema[SignedMeterValue] = DeriveSchema.gen
  given Schema[SampledValue] = DeriveSchema.gen
  given Schema[MeterValue] = DeriveSchema.gen
  given Schema[TransactionInfo] = DeriveSchema.gen
  given Schema[TransactionEventRequest] = DeriveSchema.gen
  given Schema[ConnectorStatusOcpp21] = DeriveSchema.gen
  given Schema[StatusNotificationRequest] = DeriveSchema.gen
  given Schema[HashAlgorithmType] = DeriveSchema.gen
  given Schema[OCSPRequestData] = DeriveSchema.gen
  given Schema[AuthorizationStatus] = DeriveSchema.gen
  given Schema[MessageFormat] = DeriveSchema.gen
  given Schema[MessageContent] = DeriveSchema.gen
  given Schema[IdTokenInfo] = DeriveSchema.gen
  given Schema[AuthorizeCertificateStatus] = DeriveSchema.gen
  given Schema[AuthorizeRequest] = DeriveSchema.gen
  given Schema[AuthorizeResponse] = DeriveSchema.gen
  given Schema[DERControlType] = DeriveSchema.gen
  // Schema[DERControlRequest] omitted: DERControlData = Map[String, Json] is not
  // derivable by ZIO Schema (circe Json has no ZIO Schema instance).
  // DER control payloads are handled as raw circe Json at the processor layer.
  given Schema[AFRRSignalRequest] = DeriveSchema.gen
  given Schema[AFRRSignalResponse] = DeriveSchema.gen
  given Schema[GenericStatus] = DeriveSchema.gen
  given Schema[NotifyDERStartStopRequest] = DeriveSchema.gen
  given Schema[BatterySwapEventType] = DeriveSchema.gen
  given Schema[BatteryData] = DeriveSchema.gen
  given Schema[BatterySwapRequest] = DeriveSchema.gen
  given Schema[GenericDeviceModelStatus] = DeriveSchema.gen
  given Schema[GenericDeviceModelStatusResponse] = DeriveSchema.gen
