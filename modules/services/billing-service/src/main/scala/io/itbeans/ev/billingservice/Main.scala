package io.itbeans.ev.billingservice

import io.itbeans.ev.kafka.KafkaConfig
import io.itbeans.ev.otel.{EvTracing, OtelConfig, OtelLayer}
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http._
import zio.logging.backend.SLF4J
import zio.metrics.connectors.{prometheus, MetricsConfig}
import zio.metrics.connectors.prometheus.PrometheusPublisher

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

  private val metricsServer: ZIO[PrometheusPublisher, Throwable, Any] =
    ZIO.serviceWithZIO[PrometheusPublisher] { publisher =>
      Server
        .serve(Routes(Method.GET / "metrics" ->
          Handler.fromZIO(publisher.get.map(Response.text))))
        .provide(Server.defaultWithPort(8888))
        .forkDaemon
    }

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (metricsServer *> program).provide(
      Runtime.setConfigProvider(TypesafeConfigProvider.fromResourcePath()),
      // ── Config layers ──────────────────────────────────────────────────
      ZLayer.fromZIO(ZIO.config[KafkaConfig]),
      ZLayer.fromZIO(ZIO.config[BillingConfig]),
      ZLayer.fromZIO(ZIO.config[OtelConfig]),
      OtelLayer.live,
      EvTracing.live,
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
      Server.defaultWithPort(8080),
      ZLayer.succeed(MetricsConfig(5.seconds)),
      prometheus.publisherLayer,
      prometheus.prometheusLayer
    )

  private val program: ZIO[
    BillingService & BillingKafkaConsumer & BillingPeriodicTask &
      StripeClient & InvoiceRepository & BillingAccountRepository &
      BillingTransferRepository & BillingUserRepository & BillingGrpcHandler &
      BillingConfig & Server & EvTracing,
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
      // Start gRPC server in background
      _ <- BillingGrpcTransport.start
        .tapError(err => ZIO.logError(s"Billing gRPC server error: ${err.getMessage}"))
        .ignore
        .forkDaemon
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
