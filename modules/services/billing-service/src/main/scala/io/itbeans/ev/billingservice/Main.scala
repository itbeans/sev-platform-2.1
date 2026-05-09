package io.itbeans.ev.billingservice

import io.itbeans.ev.kafka.KafkaConfig
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.logging.backend.SLF4J

// ---------------------------------------------------------------------------
// ev-billing-service — Stripe invoicing + periodic billing + CDR fund dispatch.
//
// Responsibilities:
//   • Kafka consumer: transactions.lifecycle → create Stripe draft invoice on Stop
//   • Periodic task: monthly invoice charge + fund dispatch to CPO accounts
//   • HTTP: POST /api/billing/webhook (Stripe webhook)
//   • HTTP: GET/POST /api/billing/{invoices,customer,accounts}
//   • gRPC (pre-codegen): GetInvoices, CreateBillingUser, CheckPaymentMethods
//
// Database: PostgreSQL (Doobie) — billing_invoices, billing_accounts,
//           billing_transfers, billing_users tables
// ---------------------------------------------------------------------------

object Main extends ZIOAppDefault:

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    program.provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[BillingConfig]),
      // ── Stripe client ──────────────────────────────────────────────────
      StripeClient.live,
      // ── Repository layers (PostgreSQL/Doobie) ──────────────────────────
      BillingRepository.invoiceRepoLive,
      BillingRepository.accountRepoLive,
      BillingRepository.transferRepoLive,
      BillingRepository.userRepoLive,
      // ── Business logic ─────────────────────────────────────────────────
      LiveBillingService.live,
      LiveBillingPeriodicTask.live,
      LiveBillingGrpcHandler.live,
      // ── Kafka consumer ─────────────────────────────────────────────────
      LiveBillingKafkaConsumer.live,
      // ── HTTP server ────────────────────────────────────────────────────
      Server.defaultWithPort(8080)
    )

  private val program: ZIO[
    BillingService & BillingKafkaConsumer & BillingPeriodicTask &
      StripeClient & InvoiceRepository & BillingAccountRepository &
      BillingUserRepository & BillingGrpcHandler & BillingConfig & Server,
    Throwable,
    Unit
  ] =
    for
      cfg          <- ZIO.service[BillingConfig]
      consumer     <- ZIO.service[BillingKafkaConsumer]
      periodicTask <- ZIO.service[BillingPeriodicTask]
      svc          <- ZIO.service[BillingService]
      stripe       <- ZIO.service[StripeClient]
      invoiceRepo  <- ZIO.service[InvoiceRepository]
      accountRepo  <- ZIO.service[BillingAccountRepository]
      userRepo     <- ZIO.service[BillingUserRepository]
      _ <- ZIO.logInfo(
        s"ev-billing-service starting: http=:${cfg.httpPort} " +
          s"immediatePayment=${cfg.immediatePayment} " +
          s"monthlyBillingDay=${cfg.monthlyBillingDay}"
      )
      // Start Kafka consumer in background
      _ <- consumer.start
        .tapError(err => ZIO.logError(s"Billing Kafka consumer error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Start periodic billing task for the default tenant
      _ <- periodicTask.start(cfg.defaultTenantId)
        .tapError(err => ZIO.logError(s"Billing periodic task error: ${err.getMessage}"))
        .ignore
        .forkDaemon
      // Serve HTTP — blocks until shutdown
      _ <- Server.serve(
        BillingHttpServer.routes(svc, stripe, invoiceRepo, accountRepo, userRepo, cfg)
      )
    yield ()
