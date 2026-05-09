package io.itbeans.ev.billingservice

import io.circe.{parser, Json}
import io.circe.syntax._
import io.circe.parser.decode
import zio._

import java.net.URI
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.Base64

// ---------------------------------------------------------------------------
// StripeClient — direct Stripe REST API v1 client (no Java SDK).
//
// All amounts in minor currency units (cents).
// Stripe uses application/x-www-form-urlencoded for POST/DELETE.
// Authorization: Basic {base64(secret_key + ":")}
// ---------------------------------------------------------------------------

trait StripeClient:
  // ── Customers ──────────────────────────────────────────────────────────
  def createCustomer(email: String, name: String, userId: String): Task[Json]
  def updateCustomer(customerId: String, fields: Map[String, String]): Task[Json]
  def retrieveCustomer(customerId: String): Task[Json]
  // ── Payment methods ─────────────────────────────────────────────────────
  def listPaymentMethods(customerId: String): Task[Json]
  def attachPaymentMethod(paymentMethodId: String, customerId: String): Task[Json]
  def detachPaymentMethod(paymentMethodId: String): Task[Json]
  // ── Setup intents ────────────────────────────────────────────────────────
  def createSetupIntent(customerId: String): Task[Json]
  // ── Invoices ─────────────────────────────────────────────────────────────
  def createInvoice(customerId: String, currency: String, description: String): Task[Json]
  def retrieveInvoice(invoiceId: String): Task[Json]

  def listInvoices(
      customerId: Option[String] = None,
      status: Option[String] = None,
      createdGte: Option[Long] = None,
      createdLt: Option[Long] = None,
      limit: Int = 100
  ): Task[Json]
  def finalizeInvoice(invoiceId: String): Task[Json]
  def payInvoice(invoiceId: String): Task[Json]
  def voidInvoice(invoiceId: String): Task[Json]

  // ── Invoice items ────────────────────────────────────────────────────────
  def createInvoiceItem(
      customerId: String,
      invoiceId: String,
      amountCents: Int,
      currency: String,
      description: String
  ): Task[Json]
  // ── Tax rates ────────────────────────────────────────────────────────────
  def listTaxRates(active: Boolean = true): Task[Json]
  // ── Connected accounts ───────────────────────────────────────────────────
  def createConnectedAccount(email: String, companyName: String): Task[Json]
  def createAccountLink(accountId: String, refreshUrl: String, returnUrl: String): Task[Json]

  // ── Transfers ────────────────────────────────────────────────────────────
  def createTransfer(
      amountCents: Int,
      currency: String,
      destinationAccount: String,
      description: String
  ): Task[Json]
  // ── Webhook ──────────────────────────────────────────────────────────────
  def constructEvent(payload: String, sigHeader: String): Task[Json]

// ── Live implementation ───────────────────────────────────────────────────

final class LiveStripeClient(cfg: BillingConfig) extends StripeClient:

  private val http = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  private val baseUrl = "https://api.stripe.com/v1"

  // Basic auth: base64("sk_...:")
  private val authHeader: String =
    "Basic " + Base64.getEncoder.encodeToString(s"${cfg.stripeSecretKey}:".getBytes)

  // ── Customers ────────────────────────────────────────────────────────────

  def createCustomer(email: String, name: String, userId: String): Task[Json] =
    post(
      "/customers",
      Map(
        "email"            -> email,
        "name"             -> name,
        "metadata[userId]" -> userId
      )
    )

  def updateCustomer(customerId: String, fields: Map[String, String]): Task[Json] =
    post(s"/customers/$customerId", fields)

  def retrieveCustomer(customerId: String): Task[Json] =
    get(s"/customers/$customerId")

  // ── Payment methods ───────────────────────────────────────────────────────

  def listPaymentMethods(customerId: String): Task[Json] =
    get(s"/customers/$customerId/payment_methods?type=card")

  def attachPaymentMethod(paymentMethodId: String, customerId: String): Task[Json] =
    post(s"/payment_methods/$paymentMethodId/attach", Map("customer" -> customerId))

  def detachPaymentMethod(paymentMethodId: String): Task[Json] =
    post(s"/payment_methods/$paymentMethodId/detach", Map.empty)

  // ── Setup intents ─────────────────────────────────────────────────────────

  def createSetupIntent(customerId: String): Task[Json] =
    post(
      "/setup_intents",
      Map(
        "customer"               -> customerId,
        "payment_method_types[]" -> "card"
      )
    )

  // ── Invoices ─────────────────────────────────────────────────────────────

  def createInvoice(customerId: String, currency: String, description: String): Task[Json] =
    post(
      "/invoices",
      Map(
        "customer"     -> customerId,
        "currency"     -> currency,
        "auto_advance" -> "false",
        "description"  -> description
      )
    )

  def retrieveInvoice(invoiceId: String): Task[Json] =
    get(s"/invoices/$invoiceId")

  def listInvoices(
      customerId: Option[String] = None,
      status: Option[String] = None,
      createdGte: Option[Long] = None,
      createdLt: Option[Long] = None,
      limit: Int = 100
  ): Task[Json] =
    val params = Seq(
      customerId.map("customer" -> _),
      status.map("status" -> _),
      createdGte.map("created[gte]" -> _.toString),
      createdLt.map("created[lt]" -> _.toString),
      Some("limit" -> limit.toString)
    ).flatten.toMap
    get(s"/invoices?${encodeForm(params)}")

  def finalizeInvoice(invoiceId: String): Task[Json] =
    post(s"/invoices/$invoiceId/finalize", Map.empty)

  def payInvoice(invoiceId: String): Task[Json] =
    post(s"/invoices/$invoiceId/pay", Map.empty)

  def voidInvoice(invoiceId: String): Task[Json] =
    post(s"/invoices/$invoiceId/void", Map.empty)

  // ── Invoice items ─────────────────────────────────────────────────────────

  def createInvoiceItem(
      customerId: String,
      invoiceId: String,
      amountCents: Int,
      currency: String,
      description: String
  ): Task[Json] =
    post(
      "/invoiceitems",
      Map(
        "customer"    -> customerId,
        "invoice"     -> invoiceId,
        "amount"      -> amountCents.toString,
        "currency"    -> currency,
        "description" -> description
      )
    )

  // ── Tax rates ─────────────────────────────────────────────────────────────

  def listTaxRates(active: Boolean = true): Task[Json] =
    get(s"/tax_rates?active=$active")

  // ── Connected accounts ────────────────────────────────────────────────────

  def createConnectedAccount(email: String, companyName: String): Task[Json] =
    post(
      "/accounts",
      Map(
        "type"                                   -> "express",
        "email"                                  -> email,
        "business_profile[name]"                 -> companyName,
        "capabilities[card_payments][requested]" -> "true",
        "capabilities[transfers][requested]"     -> "true"
      )
    )

  def createAccountLink(accountId: String, refreshUrl: String, returnUrl: String): Task[Json] =
    post(
      "/account_links",
      Map(
        "account"     -> accountId,
        "refresh_url" -> refreshUrl,
        "return_url"  -> returnUrl,
        "type"        -> "account_onboarding"
      )
    )

  // ── Transfers ─────────────────────────────────────────────────────────────

  def createTransfer(
      amountCents: Int,
      currency: String,
      destinationAccount: String,
      description: String
  ): Task[Json] =
    post(
      "/transfers",
      Map(
        "amount"      -> amountCents.toString,
        "currency"    -> currency,
        "destination" -> destinationAccount,
        "description" -> description
      )
    )

  // ── Webhook ───────────────────────────────────────────────────────────────

  /**
   * Validate Stripe webhook signature using HMAC-SHA256.
   * Stripe signature header format: "t=timestamp,v1=signature"
   */
  def constructEvent(payload: String, sigHeader: String): Task[Json] =
    ZIO.attempt {
      val parts         = sigHeader.split(",").map(_.split("=", 2)).collect { case Array(k, v) => k -> v }.toMap
      val timestamp     = parts.getOrElse("t", throw new Exception("Missing timestamp in Stripe signature"))
      val signature     = parts.getOrElse("v1", throw new Exception("Missing v1 in Stripe signature"))
      val signedPayload = s"$timestamp.$payload"
      val mac           = javax.crypto.Mac.getInstance("HmacSHA256")
      mac.init(new javax.crypto.spec.SecretKeySpec(cfg.stripeWebhookSecret.getBytes("UTF-8"), "HmacSHA256"))
      val expected = mac.doFinal(signedPayload.getBytes("UTF-8")).map("%02x".format(_)).mkString
      if !constantTimeEquals(expected, signature) then
        throw new Exception("Stripe webhook signature validation failed")
      parser.parse(payload).getOrElse(throw new Exception("Invalid JSON in Stripe event"))
    }

  private def constantTimeEquals(a: String, b: String): Boolean =
    if a.length != b.length then false
    else a.zip(b).foldLeft(0)((acc, pair) => acc | (pair._1 ^ pair._2)) == 0

  // ── HTTP helpers ──────────────────────────────────────────────────────────

  private def get(path: String): Task[Json] =
    ZIO.attemptBlocking {
      val req = HttpRequest.newBuilder(URI.create(s"$baseUrl$path"))
        .GET()
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authHeader)
        .header("Accept", "application/json")
        .build()
      parseResponse(http.send(req, HttpResponse.BodyHandlers.ofString()))
    }

  private def post(path: String, params: Map[String, String]): Task[Json] =
    ZIO.attemptBlocking {
      val body = encodeForm(params)
      val req = HttpRequest.newBuilder(URI.create(s"$baseUrl$path"))
        .POST(BodyPublishers.ofString(body))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", authHeader)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .build()
      parseResponse(http.send(req, HttpResponse.BodyHandlers.ofString()))
    }

  private def parseResponse(resp: HttpResponse[String]): Json =
    val body = resp.body()
    val json = parser.parse(body).getOrElse(throw new Exception(s"Non-JSON Stripe response: $body"))
    if resp.statusCode() >= 400 then
      val errMsg = json.hcursor.downField("error").downField("message").as[String].getOrElse(body)
      throw new Exception(s"Stripe API ${resp.statusCode()}: $errMsg")
    json

  private def encodeForm(params: Map[String, String]): String =
    params.map { case (k, v) =>
      java.net.URLEncoder.encode(k, "UTF-8") + "=" + java.net.URLEncoder.encode(v, "UTF-8")
    }.mkString("&")

object StripeClient:

  val live: ZLayer[BillingConfig, Nothing, StripeClient] =
    ZLayer.fromFunction(new LiveStripeClient(_))
