package io.itbeans.ev.auth

import io.itbeans.ev.domain.{TenantId, UserId, UserRole}
import zio._

// ---------------------------------------------------------------------------
// AuthorizationService — in-process RBAC/ABAC.
// Used directly (not via gRPC) by:
//   - ev-rest-api   (every authenticated request)
//   - ev-ocpp-processor  (OCPP Authorize, field-level filtering)
//   - ev-ocpp-gateway    (WebSocket upgrade token validation)
//
// The Auth Service (network service) handles JWT issuance and validation.
// This library handles the authorization logic once the token is validated.
// ---------------------------------------------------------------------------

/** The resource/action pair being checked. */
case class AuthRequest(
    resource: Resource,
    action: Action,
    token: UserToken
)

/** A resource in the authorization model. Mirrors AuthorizationsDefinition.ts Entity enum. */
enum Resource:

  // Core EV resources
  case ChargingStation, ChargingStationTemplate, Connector, Transaction, User, Tenant,
    Site, SiteArea, Company, Tag, Car, CarCatalog, Asset, Consumption,
    // Billing / payment
    Pricing, PricingDefinition, Billing, BillingAccount, BillingTransfer, Invoice, Tax,
    PaymentMethod,
    // Notifications / comms
    Notification, Connection, Source,
    // Audit / reporting
    Log, Logging, Statistics, Statistic, Report,
    // Identity / access
    RegistrationToken, OcpiEndpoint, OicpEndpoint, Setting,
    // Site membership
    UserSite, SiteUser,
    // Smart charging / profiles
    ChargingProfile, SmartCharging,
    // Async / background
    AsyncTask

/** CRUD + domain actions. Mirrors AuthorizationsDefinition.ts Action enum. */
enum Action:

  // Standard CRUD
  case Read, Create, Update, Delete, List,
    // Supplemental CRUD
    Export, Import, InError, Download,
    Assign, Unassign, UpdateByVisualId, Replace, Revoke,
    Synchronize, SynchronizeBillingUser,
    // Charging station remote commands
    Reset, ClearCache, GetConfiguration, ChangeConfiguration, SetConfiguration,
    ChangeAvailability,
    RemoteStart, RemoteStop,
    RemoteStartTransaction, RemoteStopTransaction,
    StartTransaction, StopTransaction,
    UnlockConnector, Authorize, GetDiagnostics, UpdateFirmware,
    TriggerMessage, DataTransfer,
    GetCompositeSchedule, SetChargingProfile, ClearChargingProfile,
    ExportOcppParams, GenerateQrCode, MaintainPricingDefinitions,
    GetStatusNotification, GetBootNotification,
    GetConnectorQrCode, PushTransactionCdr,
    ViewUserData, ReserveNow,
    // Transaction queries
    GetChargingStationTransactions, GetActiveTransaction, GetCompletedTransaction,
    GetRefundableTransaction, GetRefundReport, ExportCompletedTransaction,
    // Site membership
    AssignSitesToUser, UnassignSitesFromUser,
    AssignUsersToSite, UnassignUsersFromSite,
    AssignUnassignUsers, AssignUnassignSites,
    // Site area assignments
    AssignAssets, UnassignAssets, ReadAssets,
    AssignChargingStations, UnassignChargingStations,
    ReadChargingStationsFromSiteArea,
    // OCPP 2.1 additions
    SetDERControl, GetDERControl, SendAFRRSignal,
    // Smart Charging
    TriggerSmartCharging, CheckConnection,
    // Billing
    SynchronizeBilling, ForceSynchronize,
    BillingSetupPaymentMethod, BillingDeletePaymentMethod, BillingChargeInvoice,
    BillingAccountActivate, BillingAccountOnboard,
    BillingFinalizeTransfer, BillingSendTransfer,
    // Consumption / asset
    ReadConsumption, CreateConsumption, RetrieveConsumption,
    // Refund
    Refund, RefundTransaction,
    // Misc
    GenerateLocalToken, Register, TriggerJob, Ping

/** Result of an authorization check. */
sealed trait AuthResult
case object Authorized                               extends AuthResult
case class Denied(reason: String)                    extends AuthResult
case class AuthFilter(mongoFilter: Map[String, Any]) extends AuthResult

/** Service interface — use ZLayer to inject. */
trait AuthorizationService:
  def authorize(req: AuthRequest): UIO[AuthResult]
  def buildFilter(req: AuthRequest): UIO[Option[AuthFilter]]
  def projectFields(resource: Resource, token: UserToken): UIO[Set[String]]

object AuthorizationService:

  def authorize(req: AuthRequest): URIO[AuthorizationService, AuthResult] =
    ZIO.serviceWithZIO[AuthorizationService](_.authorize(req))

  def buildFilter(req: AuthRequest): URIO[AuthorizationService, Option[AuthFilter]] =
    ZIO.serviceWithZIO[AuthorizationService](_.buildFilter(req))
