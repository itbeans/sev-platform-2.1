package io.itbeans.ev.roaming

import zio._

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// CdrService — builds OCPI 2.1.1 and OICP 2.3.0 CDRs from a completed
// transaction.
//
// Called by RoamingKafkaConsumer on TransactionAction.Stop events.
// The CDR is then pushed to all registered roaming partners.
// ---------------------------------------------------------------------------

final class CdrService:

  /**
   * Build an OCPI 2.1.1 CDR from a completed roaming transaction.
   *
   * CDR fields:
   *   - total_energy: meters consumed (kWh) = (meterStop - meterStart) / 1000
   *   - total_time:   session duration (hours)
   *   - total_parking_time: inactivity duration (hours)
   *   - charging_periods: one period spanning the full session with ENERGY + TIME dimensions
   */
  def buildOcpiCdr(
      tx: RoamingTransaction,
      currencyCode: String,
      stationLocation: Option[StationLocation] = None
  ): OcpiCdr =
    val startTs    = tx.startDate
    val stopTs     = tx.endDate
    val energyKwh  = math.max(0.0, (tx.meterStop - tx.meterStart) / 1000.0)
    val totalHours = tx.totalDurationSecs.toDouble / 3600.0
    val parkingHours =
      if tx.totalInactivitySecs > 0 then Some(tx.totalInactivitySecs.toDouble / 3600.0)
      else None
    val cdrId = tx.ocpiSessionId.getOrElse(s"CDR-${tx.id}")

    val dims = List(
      OcpiCdrDimension(`type` = "ENERGY", volume = energyKwh),
      OcpiCdrDimension(`type` = "TIME", volume = totalHours)
    ) ++ parkingHours.map(h => OcpiCdrDimension(`type` = "PARKING_TIME", volume = h))

    val period = OcpiChargingPeriod(
      start_date_time = startTs,
      dimensions = dims,
      tariff_id = None
    )

    val location = buildCdrLocation(tx, stationLocation)

    OcpiCdr(
      id = cdrId,
      start_date_time = startTs,
      stop_date_time = stopTs,
      auth_id = tx.tagId.getOrElse(tx.userId.getOrElse("UNKNOWN")),
      auth_method = "AUTH_REQUEST",
      location = location,
      currency = currencyCode,
      tariffs = Nil,
      charging_periods = List(period),
      total_cost = tx.totalCost.getOrElse(0.0),
      total_energy = energyKwh,
      total_time = totalHours,
      total_parking_time = parkingHours,
      remark = None,
      last_updated = Instant.now()
    )

  /**
   * Build an OICP 2.3.0 CDR from a completed transaction.
   */
  def buildOicpCdr(tx: RoamingTransaction, operatorId: String): OicpChargeDetailRecord =
    val energyKwh = math.max(0.0, (tx.meterStop - tx.meterStart) / 1000.0)
    val identification =
      tx.tagId match
        case Some(tag) =>
          OicpIdentification(
            RFIDMifareFamilyIdentification = Some(OicpRfidId(UID = tag)),
            RemoteIdentification = None
          )
        case None =>
          OicpIdentification(
            RFIDMifareFamilyIdentification = None,
            RemoteIdentification = tx.userId.map(uid => OicpRemoteId(EvcoID = uid))
          )

    OicpChargeDetailRecord(
      SessionID = tx.ocpiSessionId.getOrElse(s"$operatorId*${tx.id}"),
      CPOPartnerSessionID = None,
      EMPPartnerSessionID = None,
      EvseID = tx.ocpiEvseId.getOrElse(s"$operatorId*E${tx.chargingStationId}*${tx.connectorId}"),
      Identification = identification,
      SessionStart = tx.startDate,
      SessionEnd = tx.endDate,
      MeterValueStart = tx.meterStart / 1000.0, // Wh → kWh
      MeterValueEnd = tx.meterStop / 1000.0,
      ConsumedEnergy = energyKwh,
      ChargingStart = tx.startDate,
      ChargingEnd = tx.endDate,
      HubOperatorID = Some(operatorId),
      HubProviderID = None
    )

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def buildCdrLocation(
      tx: RoamingTransaction,
      loc: Option[StationLocation]
  ): OcpiCdrLocation =
    val (standard, format, powerType) =
      OcpiConnectorMapping.toOcpi(loc.flatMap(_.connectorType))

    OcpiCdrLocation(
      id = tx.siteId.getOrElse(tx.chargingStationId),
      name = loc.flatMap(_.siteName),
      address = loc.map(_.address).getOrElse(""),
      city = loc.map(_.city).getOrElse(""),
      postal_code = loc.map(_.postalCode).getOrElse(""),
      country = loc.map(_.country).getOrElse("DEU"),
      coordinates = OcpiGeoLocation(
        loc.map(_.latitude).getOrElse("0.0"),
        loc.map(_.longitude).getOrElse("0.0")
      ),
      evse_uid = tx.ocpiEvseId.getOrElse(s"${tx.chargingStationId}_${tx.connectorId}"),
      evse_id = tx.ocpiEvseId.getOrElse(s"${tx.chargingStationId}*${tx.connectorId}"),
      connector_id = tx.connectorId.toString,
      connector_standard = standard,
      connector_format = format,
      connector_power_type = powerType
    )

object CdrService:

  val live: ZLayer[Any, Nothing, CdrService] =
    ZLayer.succeed(new CdrService)
