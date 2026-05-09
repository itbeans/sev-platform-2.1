package io.itbeans.ev.billingservice

import zio._

// ---------------------------------------------------------------------------
// BillingConfig — ZIO Config derivation for ev-billing-service.
// ---------------------------------------------------------------------------

case class BillingConfig(
    // HTTP / gRPC ports
    httpPort: Int,
    grpcPort: Int,
    // Stripe credentials
    stripeSecretKey: String,     // sk_live_... or sk_test_...
    stripeWebhookSecret: String, // whsec_... for webhook signature validation
    // Billing behaviour
    immediatePayment: Boolean,       // true = finalize+pay on transaction stop; false = monthly
    monthlyBillingDay: Int,          // day of month for periodic charge task (1-28)
    platformFeePercent: Double,      // % of transaction amount kept as platform fee (e.g. 2.5)
    platformFeeCentsPerSession: Int, // flat platform fee per session in minor currency units
    // Database
    jdbcUrl: String, // jdbc:postgresql://host:5432/ev_billing
    jdbcUser: String,
    jdbcPassword: String,
    // Default tenant for single-tenant deployments
    defaultTenantId: String
)

object BillingConfig:

  given Config[BillingConfig] = (
    Config.int("httpPort") zip
      Config.int("grpcPort") zip
      Config.string("stripeSecretKey") zip
      Config.string("stripeWebhookSecret") zip
      Config.boolean("immediatePayment") zip
      Config.int("monthlyBillingDay") zip
      Config.double("platformFeePercent") zip
      Config.int("platformFeeCentsPerSession") zip
      Config.string("jdbcUrl") zip
      Config.string("jdbcUser") zip
      Config.string("jdbcPassword") zip
      Config.string("defaultTenantId")
  ).nested("billing").map {
    case (
          httpPort,
          grpcPort,
          stripeKey,
          webhookSecret,
          immediatePayment,
          billingDay,
          feePercent,
          feeCents,
          jdbcUrl,
          jdbcUser,
          jdbcPassword,
          tenantId
        ) =>
      BillingConfig(
        httpPort,
        grpcPort,
        stripeKey,
        webhookSecret,
        immediatePayment,
        billingDay,
        feePercent,
        feeCents,
        jdbcUrl,
        jdbcUser,
        jdbcPassword,
        tenantId
      )
  }
