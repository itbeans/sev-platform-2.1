package io.itbeans.ev.billingservice

import zio._
import zio.Schedule

import java.time.{Instant, LocalDate, ZoneOffset}

// ---------------------------------------------------------------------------
// BillingPeriodicTask — runs scheduled billing operations.
//
// Two operations:
//   1. chargeInvoices (monthly) — finalize + pay all draft invoices from the
//      previous calendar month on the configured day of month.
//   2. dispatchFunds (monthly) — after invoices are charged, dispatch
//      collected funds to CPO connected accounts.
//
// Both run on a ZIO Schedule.
// ---------------------------------------------------------------------------

trait BillingPeriodicTask:
  def start(tenantId: String): Task[Unit]

final class LiveBillingPeriodicTask(
    billing: BillingService,
    accountRepo: BillingAccountRepository,
    cfg: BillingConfig
) extends BillingPeriodicTask:

  def start(tenantId: String): Task[Unit] =
    val chargeTask   = runChargeInvoices(tenantId)
    val dispatchTask = runDispatchFunds(tenantId)

    // Run both tasks on a daily check; actual billing only fires on the configured day
    val schedule = Schedule.fixed(1.hour)

    (chargeTask *> dispatchTask)
      .tapError(err => ZIO.logError(s"Billing periodic task error for $tenantId: ${err.getMessage}"))
      .ignore
      .repeat(schedule)
      .unit

  private def runChargeInvoices(tenantId: String): Task[Unit] =
    for
      now <- ZIO.clockWith(_.instant)
      today = LocalDate.ofInstant(now, ZoneOffset.UTC)
      _ <- if today.getDayOfMonth == cfg.monthlyBillingDay then
        val cutoff = LocalDate.of(today.getYear, today.getMonth, 1)
          .atStartOfDay(ZoneOffset.UTC).toInstant
        for
          charged <- billing.chargeAllDraftsBefore(tenantId, cutoff.toEpochMilli)
          _       <- ZIO.logInfo(s"Monthly billing for $tenantId: $charged invoices charged")
        yield ()
      else ZIO.unit
    yield ()

  private def runDispatchFunds(tenantId: String): Task[Unit] =
    for
      now <- ZIO.clockWith(_.instant)
      today = LocalDate.ofInstant(now, ZoneOffset.UTC)
      // Dispatch on the day after billing (billing day + 1, or day 2 if day 1)
      dispatchDay = math.min(cfg.monthlyBillingDay + 1, 28)
      _ <- if today.getDayOfMonth == dispatchDay then
        for
          accounts <- accountRepo.findAll(tenantId)
          active = accounts.filter(_.status == BillingAccountStatus.Active)
          _ <- ZIO.logInfo(s"Fund dispatch: ${active.size} active accounts for $tenantId")
          _ <- ZIO.foreachDiscard(active) { account =>
            // Dispatch in EUR and USD — extend as needed
            ZIO.foreachDiscard(List("eur", "usd")) { currency =>
              billing.dispatchFundsForAccount(tenantId, account.id, currency)
                .tapError(err =>
                  ZIO.logError(s"Fund dispatch failed for account ${account.id}: ${err.getMessage}")
                )
                .ignore
            }
          }
        yield ()
      else ZIO.unit
    yield ()

object LiveBillingPeriodicTask:

  val live: ZLayer[BillingService & BillingAccountRepository & BillingConfig, Nothing, BillingPeriodicTask] =
    ZLayer.fromFunction(new LiveBillingPeriodicTask(_, _, _))
