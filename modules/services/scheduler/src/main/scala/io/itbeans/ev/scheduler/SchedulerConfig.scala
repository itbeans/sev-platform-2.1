package io.itbeans.ev.scheduler

import zio._
import zio.config.magnolia._

// ---------------------------------------------------------------------------
// SchedulerConfig — ZIO Config for ev-scheduler.
// ---------------------------------------------------------------------------

case class SchedulerConfig(
    // Ports (minimal HTTP health endpoint only)
    httpPort: Int,

    // ── Task intervals ───────────────────────────────────────────────────────
    // checkOfflineStationsIntervalMins: how often to sweep for offline stations
    checkOfflineStationsIntervalMins: Int,
    // closeTransactionsIntervalHours: how often to sweep for stale open transactions
    closeTransactionsIntervalHours: Int,
    // cleanupLogsIntervalHours: how often to prune old logs/performance records
    cleanupLogsIntervalHours: Int,
    // checkUserInactivityIntervalHours: how often to sweep for inactive user accounts
    checkUserInactivityIntervalHours: Int,

    // ── Retention / thresholds ────────────────────────────────────────────────
    // A station is considered offline after this many seconds without a heartbeat
    stationOfflineThresholdSecs: Int,
    // Transactions older than this many hours with no stop document are force-closed
    staleTransactionThresholdHours: Int,
    // Log and performance records older than this many days are deleted
    logRetentionDays: Int,
    // User accounts inactive for this many months receive a notification
    userInactivityMonths: Int,

    // ── Feature flags ─────────────────────────────────────────────────────────
    offlineStationsTaskEnabled: Boolean,
    closeTransactionsTaskEnabled: Boolean,
    cleanupLogsTaskEnabled: Boolean,
    userInactivityTaskEnabled: Boolean,

    // ── Distributed lock TTL ──────────────────────────────────────────────────
    // Lock duration in seconds — prevents concurrent execution across pods
    lockDurationSecs: Int,

    // ── Tenant ───────────────────────────────────────────────────────────────
    defaultTenantId: String
)

object SchedulerConfig:

  given Config[SchedulerConfig] = (
    Config.int("httpPort") zip
      Config.int("checkOfflineStationsIntervalMins") zip
      Config.int("closeTransactionsIntervalHours") zip
      Config.int("cleanupLogsIntervalHours") zip
      Config.int("checkUserInactivityIntervalHours") zip
      Config.int("stationOfflineThresholdSecs") zip
      Config.int("staleTransactionThresholdHours") zip
      Config.int("logRetentionDays") zip
      Config.int("userInactivityMonths") zip
      Config.boolean("offlineStationsTaskEnabled") zip
      Config.boolean("closeTransactionsTaskEnabled") zip
      Config.boolean("cleanupLogsTaskEnabled") zip
      Config.boolean("userInactivityTaskEnabled") zip
      Config.int("lockDurationSecs") zip
      Config.string("defaultTenantId")
  ).nested("scheduler").map {
    case (
          httpPort,
          checkOffline,
          closeTxs,
          cleanLogs,
          checkInact,
          offlineThresh,
          staleTxThresh,
          logRet,
          inactMonths,
          offlineEn,
          closeTxEn,
          cleanEn,
          inactEn,
          lockDur,
          tenantId
        ) =>
      SchedulerConfig(
        httpPort,
        checkOffline,
        closeTxs,
        cleanLogs,
        checkInact,
        offlineThresh,
        staleTxThresh,
        logRet,
        inactMonths,
        offlineEn,
        closeTxEn,
        cleanEn,
        inactEn,
        lockDur,
        tenantId
      )
  }
