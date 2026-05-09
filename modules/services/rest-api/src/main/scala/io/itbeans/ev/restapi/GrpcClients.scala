package io.itbeans.ev.restapi

import io.grpc.ManagedChannelBuilder
import io.itbeans.ev.auth.grpc.auth_service._
import io.itbeans.ev.billing.grpc.billing_service._
import io.itbeans.ev.ocpp.grpc.ocpp_gateway._
import zio._

import scala.concurrent.ExecutionContext

// ---------------------------------------------------------------------------
// GrpcClients — ZIO service wrappers around the generated ScalaPB stubs.
//
// Each client manages its own ManagedChannel lifecycle (created on start,
// shut down on scope close) and bridges Future-based gRPC calls to ZIO Task.
//
// Channels use plaintext (no TLS) — TLS is terminated by Istio mTLS in-cluster.
// ---------------------------------------------------------------------------

// ── Auth gRPC client ──────────────────────────────────────────────────────────

final class AuthGrpcClient(stub: AuthServiceGrpc.AuthServiceStub):

  def validateToken(jwt: String): Task[ValidateTokenResponse] =
    ZIO.fromFuture(implicit ec => stub.validateToken(ValidateTokenRequest(jwt = jwt)))

  def resolveTenant(subdomain: String): Task[ResolveTenantResponse] =
    ZIO.fromFuture(implicit ec => stub.resolveTenant(ResolveTenantRequest(subdomain = subdomain)))

  def resolveOcppAuthorization(
      tenantId: String,
      idTag: String,
      idTokenType: String,
      chargingStationId: String
  ): Task[OcppAuthorizationResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.resolveOcppAuthorization(
        OcppAuthorizationRequest(
          tenantId = tenantId,
          idTag = idTag,
          idTokenType = idTokenType,
          chargingStationId = chargingStationId
        )
      )
    )

object AuthGrpcClient:

  val live: ZLayer[RestApiConfig, Nothing, AuthGrpcClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[RestApiConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.authGrpcHost, cfg.authGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new AuthGrpcClient(AuthServiceGrpc.stub(channel))
    }.orDie

// ── OCPP Gateway gRPC client ──────────────────────────────────────────────────

final class OcppGatewayGrpcClient(stub: OcppGatewayServiceGrpc.OcppGatewayServiceStub):

  /** Send an arbitrary OCPP command to a station via the gateway. */
  def sendCommand(
      tenantId: String,
      stationId: String,
      action: String,
      payloadJson: String,
      timeoutSeconds: Int = 30
  ): Task[SendCommandResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.sendCommand(
        SendCommandRequest(
          tenantId = tenantId,
          chargingStationId = stationId,
          action = action,
          payloadJson = payloadJson,
          correlationId = java.util.UUID.randomUUID().toString,
          timeoutSeconds = timeoutSeconds
        )
      )
    )

  def isStationConnected(tenantId: String, stationId: String): Task[IsStationConnectedResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.isStationConnected(
        IsStationConnectedRequest(tenantId = tenantId, chargingStationId = stationId)
      )
    )

  def listConnectedStations(tenantId: String): Task[ListConnectedStationsResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.listConnectedStations(ListConnectedStationsRequest(tenantId = tenantId))
    )

object OcppGatewayGrpcClient:

  val live: ZLayer[RestApiConfig, Nothing, OcppGatewayGrpcClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[RestApiConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.ocppGatewayGrpcHost, cfg.ocppGatewayGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new OcppGatewayGrpcClient(OcppGatewayServiceGrpc.stub(channel))
    }.orDie

// ── Billing gRPC client ───────────────────────────────────────────────────────

final class BillingGrpcClient(stub: BillingServiceGrpc.BillingServiceStub):

  def checkBillingPrerequisites(
      tenantId: String,
      userId: String,
      stationId: String
  ): Task[CheckTransactionBillingPrerequisitesResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.checkTransactionBillingPrerequisites(
        CheckTransactionBillingPrerequisitesRequest(
          tenantId = tenantId,
          userId = userId,
          chargingStationId = stationId
        )
      )
    )

  def synchronizeBillingUser(tenantId: String, userId: String): Task[SynchronizeBillingUserResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.synchronizeBillingUser(SynchronizeBillingUserRequest(tenantId = tenantId, userId = userId))
    )

  def forceSynchronizeUser(tenantId: String, userId: String): Task[ForceSynchronizeUserResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.forceSynchronizeUser(ForceSynchronizeUserRequest(tenantId = tenantId, userId = userId))
    )

  def getPaymentMethods(tenantId: String, userId: String): Task[GetPaymentMethodsResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.getPaymentMethods(GetPaymentMethodsRequest(tenantId = tenantId, userId = userId))
    )

  def setupPaymentMethod(
      tenantId: String,
      userId: String,
      paymentMethodId: String
  ): Task[SetupPaymentMethodResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.setupPaymentMethod(
        SetupPaymentMethodRequest(
          tenantId = tenantId,
          userId = userId,
          paymentMethodId = paymentMethodId
        )
      )
    )

  def deletePaymentMethod(
      tenantId: String,
      userId: String,
      paymentMethodId: String
  ): Task[DeletePaymentMethodResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.deletePaymentMethod(
        DeletePaymentMethodRequest(
          tenantId = tenantId,
          userId = userId,
          paymentMethodId = paymentMethodId
        )
      )
    )

  def chargeInvoice(tenantId: String, invoiceId: String, userId: String): Task[ChargeInvoiceResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.chargeInvoice(
        ChargeInvoiceRequest(tenantId = tenantId, invoiceId = invoiceId, userId = userId)
      )
    )

  def downloadInvoice(tenantId: String, invoiceId: String): Task[DownloadInvoiceResponse] =
    ZIO.fromFuture(implicit ec =>
      stub.downloadInvoice(DownloadInvoiceRequest(tenantId = tenantId, invoiceId = invoiceId))
    )

object BillingGrpcClient:

  val live: ZLayer[RestApiConfig, Nothing, BillingGrpcClient] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[RestApiConfig]
        channel <- ZIO.acquireRelease(
          ZIO.attempt(
            ManagedChannelBuilder
              .forAddress(cfg.billingGrpcHost, cfg.billingGrpcPort)
              .usePlaintext()
              .build()
          )
        )(ch => ZIO.attempt(ch.shutdown()).ignore)
      yield new BillingGrpcClient(BillingServiceGrpc.stub(channel))
    }.orDie
