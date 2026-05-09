package io.itbeans.ev.billingservice

import io.circe.syntax._
import io.circe.parser.decode
import sttp.tapir.ztapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.Schema
import zio._
import zio.http.{handler, Routes}

import java.time.Instant

// Explicit Schema instances prevent E172 ambiguity between ztapir._ and generic.auto._
private given Schema[Instant] = Schema.string[Instant]
private given Schema[InvoiceStatus] = Schema.string[InvoiceStatus]
private given Schema[BillingAccountStatus] = Schema.string[BillingAccountStatus]
private given Schema[BigDecimal] = Schema.string[BigDecimal]

// ---------------------------------------------------------------------------
// BillingHttpServer — Tapir-based HTTP server for ev-billing.
//
// Endpoints:
//   POST /api/billing/webhook            — Stripe webhook event receiver
//   GET  /api/billing/invoices/{userId}  — List invoices for a user
//   POST /api/billing/accounts           — Create CPO billing account
//   GET  /api/billing/accounts/{id}      — Get billing account
//   POST /api/billing/customer           — Create/get Stripe customer for user
// ---------------------------------------------------------------------------

// ── Request/Response types ────────────────────────────────────────────────

case class CreateCustomerRequest(
    userId: String,
    email: String,
    name: String
)

object CreateCustomerRequest:
  given io.circe.Encoder[CreateCustomerRequest] = io.circe.generic.semiauto.deriveEncoder
  given io.circe.Decoder[CreateCustomerRequest] = io.circe.generic.semiauto.deriveDecoder

case class CreateAccountRequest(
    userId: String,
    email: String,
    companyName: String
)

object CreateAccountRequest:
  given io.circe.Encoder[CreateAccountRequest] = io.circe.generic.semiauto.deriveEncoder
  given io.circe.Decoder[CreateAccountRequest] = io.circe.generic.semiauto.deriveDecoder

object BillingHttpServer:

  private val base = endpoint.in("api" / "billing")

  // POST /api/billing/webhook
  private val webhookEndpoint =
    base.post
      .in("webhook")
      .in(stringBody)
      .in(header[String]("stripe-signature"))
      .out(stringBody)
      .errorOut(stringBody)

  // GET /api/billing/invoices/{tenantId}/{userId}
  private val listInvoicesEndpoint =
    base.get
      .in("invoices" / path[String]("tenantId") / path[String]("userId"))
      .out(jsonBody[List[Invoice]])
      .errorOut(stringBody)

  // POST /api/billing/customer
  private val createCustomerEndpoint =
    base.post
      .in("customer")
      .in(path[String]("tenantId"))
      .in(jsonBody[CreateCustomerRequest])
      .out(jsonBody[BillingUser])
      .errorOut(stringBody)

  // POST /api/billing/accounts
  private val createAccountEndpoint =
    base.post
      .in("accounts")
      .in(path[String]("tenantId"))
      .in(jsonBody[CreateAccountRequest])
      .out(jsonBody[BillingAccount])
      .errorOut(stringBody)

  // GET /api/billing/accounts/{tenantId}/{id}
  private val getAccountEndpoint =
    base.get
      .in("accounts" / path[String]("tenantId") / path[String]("id"))
      .out(jsonBody[BillingAccount])
      .errorOut(stringBody)

  def routes(
      billing: BillingService,
      stripe: StripeClient,
      invoiceRepo: InvoiceRepository,
      accountRepo: BillingAccountRepository,
      userRepo: BillingUserRepository,
      cfg: BillingConfig
  ): Routes[Any, zio.http.Response] =
    ZioHttpInterpreter().toHttp(List(
      webhookEndpoint.zServerLogic { case (payload, sigHeader) =>
        stripe.constructEvent(payload, sigHeader)
          .flatMap { event =>
            val eventType = event.hcursor.downField("type").as[String].getOrElse("")
            handleStripeEvent(eventType, event, billing, invoiceRepo, cfg)
          }
          .as("ok")
          .mapError(_.getMessage)
      },
      listInvoicesEndpoint.zServerLogic { case (tenantId, userId) =>
        invoiceRepo.findByUserId(tenantId, userId)
          .mapError(_.getMessage)
      },
      createCustomerEndpoint.zServerLogic { case (tenantId, body) =>
        billing.getOrCreateCustomer(tenantId, body.userId, body.email, body.name)
          .flatMap { customerId =>
            userRepo.findByUserId(tenantId, body.userId)
              .flatMap(ZIO.fromOption(_).orElseFail(new Exception("BillingUser not found after create")))
          }
          .mapError(_.getMessage)
      },
      createAccountEndpoint.zServerLogic { case (tenantId, body) =>
        (for
          stripeJson <- stripe.createConnectedAccount(body.email, body.companyName)
          externalId = stripeJson.hcursor.downField("id").as[String].getOrElse("")
          link <- stripe.createAccountLink(
            externalId,
            refreshUrl = "https://csms.example.com/billing/account/refresh",
            returnUrl = "https://csms.example.com/billing/account/return"
          ).map(_.hcursor.downField("url").as[String].toOption)
          now = java.time.Instant.now()
          account = BillingAccount(
            id = java.util.UUID.randomUUID().toString,
            tenantId = tenantId,
            businessOwnerUserId = body.userId,
            companyName = body.companyName,
            status = BillingAccountStatus.Pending,
            accountExternalId = Some(externalId),
            activationLink = link,
            createdOn = now,
            createdBy = body.userId
          )
          _ <- accountRepo.create(account)
        yield account).mapError(_.getMessage)
      },
      getAccountEndpoint.zServerLogic { case (tenantId, id) =>
        accountRepo.findById(tenantId, id)
          .flatMap(ZIO.fromOption(_).orElseFail(new Exception(s"BillingAccount $id not found")))
          .mapError(_.getMessage)
      }
    ))

  // ── Stripe webhook event dispatch ─────────────────────────────────────────

  private def handleStripeEvent(
      eventType: String,
      event: io.circe.Json,
      billing: BillingService,
      invoiceRepo: InvoiceRepository,
      cfg: BillingConfig
  ): Task[Unit] =
    eventType match
      case "invoice.payment_succeeded" =>
        val stripeInvoiceId = event.hcursor.downField("data").downField("object").downField("id").as[String].toOption
        stripeInvoiceId match
          case None => ZIO.logWarning("invoice.payment_succeeded: missing invoice id")
          case Some(id) =>
            ZIO.logInfo(s"Stripe webhook: invoice $id payment succeeded")
      // Status update is handled by chargeInvoice syncing from Stripe;
      // the webhook is a confirmation signal only.

      case "invoice.payment_failed" =>
        val stripeInvoiceId = event.hcursor.downField("data").downField("object").downField("id").as[String].toOption
        ZIO.logWarning(s"Stripe webhook: payment failed for invoice $stripeInvoiceId")

      case "account.updated" =>
        // Connected account onboarding completed — mark as active
        val accountId = event.hcursor.downField("data").downField("object").downField("id").as[String].toOption
        val chargesEnabled =
          event.hcursor.downField("data").downField("object").downField("charges_enabled").as[Boolean].getOrElse(false)
        ZIO.logInfo(s"Stripe webhook: connected account $accountId charges_enabled=$chargesEnabled")

      case other =>
        ZIO.logDebug(s"Stripe webhook: unhandled event type $other")
