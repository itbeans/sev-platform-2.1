package io.itbeans.ev.notification.email

import io.itbeans.ev.notification.{NotificationRequest, NotificationType}

// ---------------------------------------------------------------------------
// EmailTemplates — HTML email templates for each notification type.
//
// Replaces the MJML-based templates in:
//   src/assets/email/mjml-components/ (TypeScript)
//
// MJML compilation (mjml2html) requires Node.js and is not run in the JVM.
// Instead, these templates produce responsive HTML directly.  The structure
// mirrors the MJML components:
//   header (tenant branding) → title → main message → optional table → footer
//
// Each template returns (subject, htmlBody). The htmlBody is wrapped in the
// common responsive shell by EmailTemplates.resolve.
// ---------------------------------------------------------------------------

case class EmailContent(
    to: String,
    subject: String,
    htmlBody: String
)

object EmailTemplates:

  /** Resolve subject + HTML body for a notification. Returns None if no template exists yet. */
  def resolve(req: NotificationRequest): Option[EmailContent] =
    templateFor(req).map { case (subject, inner) =>
      EmailContent(
        to = req.userEmail.getOrElse(""),
        subject = subject,
        htmlBody = wrap(req, inner)
      )
    }

  // ── Per-type templates ────────────────────────────────────────────────────

  private def templateFor(req: NotificationRequest): Option[(String, String)] =
    val d = req.data
    req.notificationType match

      case NotificationType.EndOfCharge =>
        Some(
          s"Charging session complete — ${d.getOrElse("totalConsumption", "")} kWh",
          s"""<p>Your charging session has ended.</p>
             |${sessionTable(d)}""".stripMargin
        )

      case NotificationType.OptimalChargeReached =>
        Some(
          s"Optimal charge reached — ${d.getOrElse("stateOfCharge", "")}%",
          s"""<p>Your vehicle has reached its optimal charge level.</p>
             |<table class="data-table">
             |  <tr><td>Station</td><td>${d.getOrElse("chargeBoxID", "")}</td></tr>
             |  <tr><td>State of Charge</td><td>${d.getOrElse("stateOfCharge", "")}%</td></tr>
             |  <tr><td>Energy delivered</td><td>${d.getOrElse("totalConsumption", "")} kWh</td></tr>
             |</table>""".stripMargin
        )

      case NotificationType.TransactionStarted =>
        Some(
          "Charging session started",
          s"""<p>A charging session has started at station <strong>${d.getOrElse("chargeBoxID", "")}</strong>.</p>
             |<table class="data-table">
             |  <tr><td>Connector</td><td>${d.getOrElse("connectorId", "")}</td></tr>
             |  <tr><td>Started at</td><td>${d.getOrElse("startedAt", "")}</td></tr>
             |</table>""".stripMargin
        )

      case NotificationType.EndOfSession =>
        Some(
          s"Charging session ended — ${d.getOrElse("totalConsumption", "")} kWh",
          s"""<p>Your charging session has ended.</p>
             |${sessionTable(d)}""".stripMargin
        )

      case NotificationType.NewRegisteredUser =>
        Some(
          "Welcome — please verify your email address",
          s"""<p>Welcome to ${req.tenantName}!</p>
             |<p>Please verify your email address to activate your account.</p>
             |${ctaButton(d.getOrElse("verificationUrl", ""), "Verify Email")}""".stripMargin
        )

      case NotificationType.VerificationEmail =>
        Some(
          "Verify your email address",
          s"""<p>Please verify your email address to continue.</p>
             |${ctaButton(d.getOrElse("verificationUrl", ""), "Verify Email")}""".stripMargin
        )

      case NotificationType.AccountVerificationNotification =>
        val active = d.getOrElse("isAccountActivated", "false").toLowerCase == "true"
        Some(
          if active then "Your account is now active" else "Account verification pending",
          if active then
            s"""<p>Your account has been verified and is now active. You can log in and start charging.</p>
               |${ctaButton(d.getOrElse("dashboardUrl", ""), "Go to Dashboard")}""".stripMargin
          else
            s"<p>Your account is pending verification. You will receive an email once it is approved.</p>"
        )

      case NotificationType.UserAccountStatusChanged =>
        val active = d.getOrElse("active", "false").toLowerCase == "true"
        Some(
          if active then "Your account has been activated" else "Your account has been deactivated",
          if active then
            "<p>Your account is now active. You can log in and start charging.</p>"
          else
            "<p>Your account has been deactivated. Please contact support if you believe this is an error.</p>"
        )

      case NotificationType.UserAccountInactivity =>
        Some(
          "Account inactivity notice",
          s"""<p>Your account has been inactive since ${d.getOrElse("lastLoginDate", "an extended period")}.</p>
             |<p>Please log in to keep your account active.</p>
             |${ctaButton(d.getOrElse("dashboardUrl", ""), "Log In")}""".stripMargin
        )

      case NotificationType.ChargingStationStatusError =>
        Some(
          s"Station error — ${d.getOrElse("chargeBoxID", "")}",
          s"""<p>Charging station <strong>${d.getOrElse("chargeBoxID", "")}</strong> has reported an error.</p>
             |<table class="data-table">
             |  <tr><td>Connector</td><td>${d.getOrElse("connectorId", "")}</td></tr>
             |  <tr><td>Status</td><td>${d.getOrElse("status", "")}</td></tr>
             |  <tr><td>Error Code</td><td>${d.getOrElse("errorCode", "")}</td></tr>
             |  <tr><td>Info</td><td>${d.getOrElse("info", "")}</td></tr>
             |</table>""".stripMargin
        )

      case NotificationType.ChargingStationRegistered =>
        Some(
          s"New station registered — ${d.getOrElse("chargeBoxID", "")}",
          s"""<p>A new charging station has been registered on your network.</p>
             |<table class="data-table">
             |  <tr><td>Station ID</td><td>${d.getOrElse("chargeBoxID", "")}</td></tr>
             |  <tr><td>OCPP Version</td><td>${d.getOrElse("ocppVersion", "")}</td></tr>
             |</table>""".stripMargin
        )

      case NotificationType.UnknownUserBadged =>
        Some(
          s"Unknown badge at ${d.getOrElse("chargeBoxID", "")}",
          s"""<p>An unknown RFID badge was presented at station <strong>${d.getOrElse("chargeBoxID", "")}</strong>.</p>
             |<table class="data-table">
             |  <tr><td>Badge ID</td><td>${d.getOrElse("badgeID", "")}</td></tr>
             |  <tr><td>Station</td><td>${d.getOrElse("chargeBoxID", "")}</td></tr>
             |</table>""".stripMargin
        )

      case NotificationType.OfflineChargingStations =>
        val stationList = d
          .getOrElse("stationIDs", "")
          .split(",")
          .filter(_.nonEmpty)
          .map(id => s"<li>$id</li>")
          .mkString
        Some(
          "Offline charging stations detected",
          s"""<p>The following charging stations have been offline for an extended period:</p>
             |<ul>$stationList</ul>""".stripMargin
        )

      case NotificationType.BillingNewInvoice =>
        val paid   = d.getOrElse("invoiceStatus", "").equalsIgnoreCase("paid")
        val status = if paid then "Paid" else "Payment required"
        Some(
          s"Invoice ${d.getOrElse("invoiceNumber", "")} — $status",
          s"""<p>A new invoice has been generated for your account.</p>
             |<table class="data-table">
             |  <tr><td>Invoice #</td><td>${d.getOrElse("invoiceNumber", "")}</td></tr>
             |  <tr><td>Amount</td><td>${d.getOrElse("invoiceAmount", "")} ${d.getOrElse("currency", "")}</td></tr>
             |  <tr><td>Status</td><td>$status</td></tr>
             |</table>
             |${d.get("invoiceUrl").fold("")(url => ctaButton(url, "View Invoice"))}""".stripMargin
        )

      case NotificationType.BillingSynchronizationFailed =>
        Some(
          "Billing synchronization failed",
          s"""<p>An error occurred during billing synchronization.</p>
             |<p>Error: ${d.getOrElse("error", "Unknown error")}</p>""".stripMargin
        )

      case NotificationType.OcpiPatchStatusError =>
        Some(
          "OCPI status patch error",
          s"""<p>An error occurred while patching charging station statuses via OCPI.</p>
             |<p>Error: ${d.getOrElse("error", "Unknown error")}</p>""".stripMargin
        )

      case NotificationType.OicpPatchStatusError =>
        Some(
          "OICP status patch error",
          s"""<p>An error occurred while patching charging station statuses via OICP.</p>
             |<p>Error: ${d.getOrElse("error", "Unknown error")}</p>""".stripMargin
        )

      // Not yet implemented — will be added in Phase 4d when full service build-out begins.
      case _ => None

  // ── Reusable HTML fragments ───────────────────────────────────────────────

  private def sessionTable(d: Map[String, String]): String =
    s"""<table class="data-table">
       |  <tr><td>Station</td><td>${d.getOrElse("chargeBoxID", "")}</td></tr>
       |  <tr><td>Duration</td><td>${d.getOrElse("totalDuration", "")}</td></tr>
       |  <tr><td>Energy</td><td>${d.getOrElse("totalConsumption", "")} kWh</td></tr>
       |  <tr><td>Inactivity</td><td>${d.getOrElse("totalInactivity", "")}</td></tr>
       |</table>""".stripMargin

  private def ctaButton(url: String, label: String): String =
    if url.isEmpty then ""
    else
      s"""<p style="margin-top:20px;">
         |  <a href="$url"
         |     style="background:#00376C;color:#fff;padding:12px 24px;border-radius:4px;
         |            text-decoration:none;display:inline-block;font-weight:bold;">$label</a>
         |</p>""".stripMargin

  // ── Responsive HTML shell ─────────────────────────────────────────────────

  private def wrap(req: NotificationRequest, body: String): String =
    val logoHtml = req.tenantLogoUrl.fold("") { url =>
      s"""<img src="$url" alt="${req.tenantName}"
               style="max-height:48px;margin-bottom:8px;display:block;" />"""
    }
    val greeting = req.userName.fold("")(name => s"<p>Hello <strong>$name</strong>,</p>")
    s"""<!DOCTYPE html>
       |<html lang="${req.userLocale}">
       |<head>
       |  <meta charset="utf-8" />
       |  <meta name="viewport" content="width=device-width,initial-scale=1" />
       |  <style>
       |    body{font-family:Arial,sans-serif;color:#333;margin:0;padding:0;background:#f5f5f5}
       |    .container{max-width:600px;margin:24px auto;background:#fff;border-radius:8px;
       |               overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)}
       |    .header{background:#00376C;color:#fff;padding:20px 24px}
       |    .header h1{margin:0;font-size:20px;font-weight:bold}
       |    .body{padding:24px;line-height:1.6}
       |    .body p{margin:0 0 12px}
       |    table.data-table{width:100%;border-collapse:collapse;margin:16px 0}
       |    table.data-table td{padding:8px 12px;border-bottom:1px solid #eee;font-size:14px}
       |    table.data-table td:first-child{font-weight:bold;width:160px;color:#555}
       |    .footer{background:#f0f0f0;padding:14px 24px;font-size:12px;color:#888;text-align:center}
       |  </style>
       |</head>
       |<body>
       |  <div class="container">
       |    <div class="header">
       |      $logoHtml
       |      <h1>${req.tenantName}</h1>
       |    </div>
       |    <div class="body">
       |      $greeting
       |      $body
       |    </div>
       |    <div class="footer">
       |      <p>This is an automated message from ${req.tenantName}. Please do not reply.</p>
       |    </div>
       |  </div>
       |</body>
       |</html>""".stripMargin
