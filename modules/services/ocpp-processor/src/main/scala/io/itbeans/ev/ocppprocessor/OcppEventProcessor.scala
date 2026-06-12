package io.itbeans.ev.ocppprocessor

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.itbeans.ev.domain._
import io.itbeans.ev.kafka.{EvKafkaProducer, KafkaConfig, Topics}
import org.bson.Document
import zio._
import zio.kafka.consumer._
import zio.kafka.serde.Serde

import java.time.Instant
import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// OcppEventProcessor — core OCPP business logic Kafka consumer.
//
// Consumes: ocpp.events.{tenantId}  (all tenant topics via wildcard pattern)
// Produces:
//   transactions.lifecycle   — Tx start/update/end → Billing, Roaming, Analytics
//   smart-charging.triggers  — StatusNotification triggers profile recomputation
//   notifications.outbound   — Session start/end push notifications
//   audit.log                — All events logged for ev-analytics
//   consumptions.ingest      — MeterValues → ev-analytics TimescaleDB
//
// Partition key = chargingStationId, so all events for one station are
// processed in order by the same consumer instance.
// ---------------------------------------------------------------------------

final class OcppEventProcessor(
    repo: ProcessorRepository,
    producer: EvKafkaProducer,
    config: KafkaConfig,
    authClient: ProcessorAuthClient,
    gatewayClient: ProcessorGatewayClient,
    pricingClient: ProcessorPricingClient
):

  def startConsuming: Task[Unit] =
    val settings = ConsumerSettings(config.bootstrapServers)
      .withGroupId(config.groupId)
      .withProperty("auto.offset.reset", config.autoOffsetReset)
      .withProperty("enable.auto.commit", "false")

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(Subscription.pattern("ocpp\\.events\\..*".r), Serde.string, Serde.string)
          .mapZIO { record =>
            processRecord(record.key, record.value, record.record.topic())
              .tapError(err => ZIO.logError(s"[Processor] Failed: $err"))
              .ignore
              .as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

  // ── Event dispatch ───────────────────────────────────────────────────────

  def processRecord(key: String, valueJson: String, topic: String): Task[Unit] =
    val tenantIdStr = topic.stripPrefix("ocpp.events.")
    val tenantId    = TenantId(tenantIdStr)
    val stationId   = ChargingStationId(key)

    for
      json <- ZIO.fromEither(parse(valueJson)).mapError(e => new Exception(e.message))
      action <- ZIO.fromEither(
        json.hcursor.downField("action").as[String]
      ).mapError(e => new Exception(e.message))
      _ <- ZIO.logDebug(s"[Processor] $action from $stationId ($tenantIdStr)")
      _ <- Metric.counter("ocpp.events.total").tagged("action", action).increment
      _ <- action match
        case "BootNotification"              => handleBootNotification(json, tenantId, stationId)
        case "Heartbeat"                     => handleHeartbeat(json, tenantId, stationId)
        case "StatusNotification"            => handleStatusNotification(json, tenantId, stationId)
        case "Authorize"                     => handleAuthorize(json, tenantId, stationId)
        case "StartTransaction"              => handleStartTransaction(json, tenantId, stationId)  // OCPP 1.6
        case "StopTransaction"               => handleStopTransaction(json, tenantId, stationId)   // OCPP 1.6
        case "MeterValues"                   => handleMeterValues(json, tenantId, stationId)       // OCPP 1.6
        case "DataTransfer"                  => handleDataTransfer(json, tenantId, stationId)      // OCPP 1.6
        case "DiagnosticsStatusNotification" => handleDiagnosticsStatus(json, tenantId, stationId) // OCPP 1.6
        case "FirmwareStatusNotification"    => handleFirmwareStatus(json, tenantId, stationId)    // OCPP 1.6
        case "TransactionEvent"              => handleTransactionEvent(json, tenantId, stationId)  // OCPP 2.x
        case "NotifyDERStartStop"            => handleDERStartStop(json, tenantId, stationId)      // OCPP 2.1
        case "BatterySwap"                   => handleBatterySwap(json, tenantId, stationId)       // OCPP 2.1
        case "AFRRSignal"                    => handleAFRRSignal(json, tenantId, stationId)        // OCPP 2.1
        case unknown =>
          ZIO.logDebug(s"[Processor] Unhandled action '$unknown' from $stationId")
    yield ()

  // ── BootNotification ─────────────────────────────────────────────────────

  def handleBootNotification(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor = json.hcursor.downField("payload")
    val vendor = cursor.downField("chargingStation").downField("vendorName").as[String].getOrElse("Unknown")
    val model  = cursor.downField("chargingStation").downField("model").as[String].getOrElse("Unknown")
    val fw     = cursor.downField("chargingStation").downField("firmwareVersion").as[String].toOption
    val now    = java.util.Date.from(Instant.now())

    val doc = new Document("_id", stationId.value)
      .append("tenantId", tenantId.value)
      .append("chargePointVendor", vendor)
      .append("chargePointModel", model)
      .append("firmwareVersion", fw.orNull)
      .append("lastChangedOn", now)
      .append("lastHeartbeat", now)
      .append("inactive", false)

    repo.upsertStation(tenantId.value, stationId.value, doc) *>
      publishAuditLog(tenantId, stationId, "BootNotification", s"Station ${stationId.value} registered: $vendor $model")

  // ── Heartbeat ────────────────────────────────────────────────────────────

  def handleHeartbeat(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    repo.updateHeartbeat(tenantId.value, stationId.value, Instant.now())

  // ── StatusNotification ───────────────────────────────────────────────────

  def handleStatusNotification(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor      = json.hcursor.downField("payload")
    val connectorId = cursor.downField("connectorId").as[Int].getOrElse(0)
    val status = cursor.downField("connectorStatus").as[String]
      .orElse(cursor.downField("status").as[String]) // 1.6 compat
      .getOrElse("Unknown")
    val timestamp = Instant.now()

    repo.updateConnectorStatus(tenantId.value, stationId.value, connectorId, status, timestamp) *>
      // Trigger smart charging recomputation on status change
      publishSmartChargingTrigger(tenantId, stationId, "Reoptimize", Some(connectorId)) *>
      publishAuditLog(tenantId, stationId, "StatusNotification", s"Connector $connectorId status → $status")

  // ── Authorize ────────────────────────────────────────────────────────────

  def handleAuthorize(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor   = json.hcursor
    val idTag    = cursor.downField("payload").downField("idTag").as[String].getOrElse("")
    val uniqueId = cursor.downField("uniqueId").as[String].getOrElse("")
    for
      _ <- ZIO.logInfo(s"[Processor] Authorize '$idTag' from ${stationId.value}")
      resp <- authClient
        .resolveOcppAuthorization(tenantId.value, idTag, stationId.value)
        .tapError(err => ZIO.logWarning(s"[Processor] Auth gRPC failed for Authorize: $err"))
        .orElseSucceed(
          // If auth service unreachable, default to Invalid so station is not unblocked
          io.itbeans.ev.auth.grpc.auth_service.OcppAuthorizationResponse(status = "Invalid")
        )
      responseJson = io.circe.Json.obj(
        "idTagInfo" -> io.circe.Json.obj(
          "status" -> io.circe.Json.fromString(resp.status)
        )
      ).noSpaces
      _ <- gatewayClient
        .sendResponse(tenantId.value, stationId.value, uniqueId, responseJson)
        .tapError(err => ZIO.logWarning(s"[Processor] Gateway gRPC failed for AuthorizeResponse: $err"))
        .ignore
      _ <- publishAuditLog(tenantId, stationId, "Authorize", s"idTag=$idTag status=${resp.status}")
    yield ()

  // ── StartTransaction (OCPP 1.6) ──────────────────────────────────────────

  def handleStartTransaction(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor = json.hcursor.downField("payload")
    val txIdStr =
      cursor.downField("transactionId").as[Long].map(_.toString).getOrElse(java.util.UUID.randomUUID().toString)
    val connectorId = cursor.downField("connectorId").as[Int].getOrElse(1)
    val meterStart  = cursor.downField("meterStart").as[Double].getOrElse(0.0)
    val tagId       = cursor.downField("idTag").as[String].toOption
    val now         = java.util.Date.from(Instant.now())

    for
      // Resolve userId via auth service; fall back to tagId if unreachable
      authResp <- authClient
        .resolveOcppAuthorization(tenantId.value, tagId.getOrElse(""), stationId.value)
        .tapError(err => ZIO.logWarning(s"[Processor] Auth lookup failed for tx $txIdStr: ${err.getMessage}"))
        .option
      resolvedUserId = authResp.filter(_.status == "Accepted")
        .map(_.userId).filter(_.nonEmpty).orElse(tagId)
      // Get connector info for tariff matching
      stationDoc <- repo.getStation(tenantId.value, stationId.value)
      (connectorType, connectorPowerKw) = extractConnectorInfo(stationDoc, connectorId)
      pricingOpt <- pricingClient
        .resolvePricing(
          tenantId.value,
          stationId.value,
          connectorId.toString,
          connectorType,
          connectorPowerKw,
          resolvedUserId.getOrElse(""),
          now.getTime
        )
        .tapError(err => ZIO.logWarning(s"[Processor] ResolvePricing failed for tx $txIdStr: ${err.getMessage}"))
        .option
      doc = new Document("_id", txIdStr)
        .append("tenantId", tenantId.value)
        .append("chargingStationId", stationId.value)
        .append("connectorId", connectorId)
        .append("tagId", tagId.orNull)
        .append("userId", resolvedUserId.orNull)
        .append("connectorType", connectorType)
        .append("connectorPowerKw", connectorPowerKw)
        .append("meterStart", meterStart)
        .append("pricingModelJson", pricingOpt.filter(_.found).map(_.pricingModelJson).orNull)
        .append("pricingCurrency", pricingOpt.filter(_.found).map(_.currency).orNull)
        .append("timestamp", now)
        .append("startDate", now)
        .append("inProgress", true)
        .append("createdOn", now)
        .append("lastChangedOn", now)
      _ <- repo.createTransaction(tenantId.value, doc)
      _ <-
        publishTransactionLifecycle(tenantId, stationId, txIdStr, "Start", connectorId, resolvedUserId, meterStart, now)
      _ <- publishAuditLog(
        tenantId,
        stationId,
        "StartTransaction",
        s"Transaction $txIdStr started on connector $connectorId"
      )
    yield ()

  // ── StopTransaction (OCPP 1.6) ───────────────────────────────────────────

  def handleStopTransaction(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor    = json.hcursor.downField("payload")
    val txIdStr   = cursor.downField("transactionId").as[Long].map(_.toString).getOrElse("")
    val meterStop = cursor.downField("meterStop").as[Double].getOrElse(0.0)
    val reason    = cursor.downField("reason").as[String].getOrElse("Local")
    val now       = java.util.Date.from(Instant.now())

    val closeDoc = new Document()
      .append("endDate", now)
      .append("meterStop", meterStop)
      .append("stopReason", reason)
      .append("lastChangedOn", now)

    for
      _        <- repo.closeTransaction(tenantId.value, txIdStr, closeDoc)
      txDocOpt <- repo.getTransaction(tenantId.value, txIdStr)
      (meterStartWh, startEpochMs, userId, pricingModelJson, connectorType, connectorPowerKw, connectorId) =
        txDocOpt match
          case None => (0.0, now.getTime, None, None, "", 0.0, 1)
          case Some(d) =>
            val ms  = Option(d.get("meterStart")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
            val sd  = Option(d.getDate("startDate")).map(_.getTime).getOrElse(now.getTime)
            val uid = Option(d.getString("userId")).orElse(Option(d.getString("tagId")))
            val pmj = Option(d.getString("pricingModelJson")).filter(_.nonEmpty)
            val ct  = Option(d.getString("connectorType")).getOrElse("")
            val cpk = Option(d.get("connectorPowerKw")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
            val cid = Option(d.get("connectorId")).flatMap(_.toString.toIntOption).getOrElse(1)
            (ms, sd, uid, pmj, ct, cpk, cid)
      consumptionWh = math.max(0.0, meterStop - meterStartWh)
      durationSecs  = math.max(0L, (now.getTime - startEpochMs) / 1000)
      finalPriceOpt <- pricingModelJson match
        case None => ZIO.succeed(None)
        case Some(pmj) =>
          pricingClient
            .finalisePrice(
              tenantId.value,
              txIdStr,
              pmj,
              consumptionWh,
              connectorType,
              connectorPowerKw,
              startEpochMs,
              now.getTime
            )
            .map(Some(_))
            .tapError(err => ZIO.logWarning(s"[Processor] FinalisePrice failed for tx $txIdStr: ${err.getMessage}"))
            .option
            .map(_.flatten)
      _ <- finalPriceOpt.fold(ZIO.unit) { fp =>
        repo.updateTransaction(
          tenantId.value,
          txIdStr,
          new Document("totalCost", fp.totalPrice)
            .append("pricingCurrency", fp.currency)
            .append("invoiceItemJson", fp.invoiceItem)
        )
      }
      _ <- publishTransactionLifecycleEnded(
        tenantId,
        stationId,
        txIdStr,
        connectorId,
        reason,
        meterStartWh,
        meterStop,
        startEpochMs,
        now.getTime,
        consumptionWh,
        durationSecs,
        userId,
        finalPriceOpt
      )
      _ <- publishAuditLog(tenantId, stationId, "StopTransaction", s"Transaction $txIdStr stopped: $reason")
    yield ()

  // ── MeterValues (OCPP 1.6) ───────────────────────────────────────────────

  def handleMeterValues(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor      = json.hcursor.downField("payload")
    val connectorId = cursor.downField("connectorId").as[Int].getOrElse(0)
    val txId        = cursor.downField("transactionId").as[Long].toOption
    val now         = java.util.Date.from(Instant.now())

    // Parse all sampled values from the first meter value entry
    val sampledValues = cursor
      .downField("meterValue")
      .downArray
      .downField("sampledValue")
      .as[Vector[Json]]
      .getOrElse(Vector.empty)

    // Find power reading (Energy.Active.Import.Rate or unit W/kW)
    val instantWatts = sampledValues
      .find { sv =>
        val measurand = sv.hcursor.downField("measurand").as[String].getOrElse("")
        val unit      = sv.hcursor.downField("unit").as[String].getOrElse("")
        measurand == "Power.Active.Import" || unit == "W"
      }
      .orElse(sampledValues.headOption)
      .flatMap(_.hcursor.downField("value").as[String].toOption)
      .flatMap(_.toDoubleOption)
      .getOrElse(0.0)

    // Find cumulative energy register reading (kWh since meter installation)
    val energyRegisterWh: Option[Double] = sampledValues
      .find { sv =>
        val measurand = sv.hcursor.downField("measurand").as[String].getOrElse("")
        val unit      = sv.hcursor.downField("unit").as[String].getOrElse("")
        measurand == "Energy.Active.Import.Register" || unit == "Wh"
      }
      .flatMap(_.hcursor.downField("value").as[String].toOption)
      .flatMap(_.toDoubleOption)

    for
      // Compute cumulatedKwh = energy consumed since tx start
      // Use absolute register reading minus meterStart when available,
      // otherwise fall back to the last recorded value for the transaction.
      cumulatedKwh <- txId match
        case None => ZIO.succeed(0.0)
        case Some(txIdLong) =>
          energyRegisterWh match
            case Some(registerWh) =>
              // Subtract meterStart (Wh) from the transaction document
              repo.getTransaction(tenantId.value, txIdLong.toString).map {
                case Some(txDoc) =>
                  val meterStartWh = Option(txDoc.get("meterStart"))
                    .map(_.toString.toDoubleOption.getOrElse(0.0))
                    .getOrElse(0.0)
                  math.max(0.0, (registerWh - meterStartWh) / 1000.0)
                case None => registerWh / 1000.0
              }
            case None =>
              repo.getLastCumulatedKwh(tenantId.value, txIdLong.toString)

      doc = new Document()
        .append("tenantId", tenantId.value)
        .append("chargingStationId", stationId.value)
        .append("connectorId", connectorId)
        .append("transactionId", txId.map(Long.box).orNull)
        .append("time", now)
        .append("instantWatts", instantWatts)
        .append("cumulatedKwh", cumulatedKwh)

      _ <- repo.insertConsumption(tenantId.value, doc)
      _ <- publishConsumptionIngest(tenantId, stationId, connectorId, txId, instantWatts, cumulatedKwh)

      // Update real-time pricing for running transactions (fail-silent)
      _ <- txId match
        case None => ZIO.unit
        case Some(txIdLong) =>
          repo.getTransaction(tenantId.value, txIdLong.toString).flatMap {
            case None => ZIO.unit
            case Some(txDoc) =>
              Option(txDoc.getString("pricingModelJson")).filter(_.nonEmpty) match
                case None => ZIO.unit
                case Some(pmj) =>
                  val intervalEndMs   = now.getTime
                  val intervalStartMs = intervalEndMs - 60000L
                  val connectorType   = Option(txDoc.getString("connectorType")).getOrElse("")
                  val connectorPowerKw =
                    Option(txDoc.get("connectorPowerKw")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
                  pricingClient
                    .priceConsumption(
                      tenantId.value,
                      txIdLong.toString,
                      pmj,
                      cumulatedKwh * 1000.0,
                      instantWatts,
                      connectorType,
                      connectorPowerKw,
                      intervalStartMs,
                      intervalEndMs
                    )
                    .flatMap { r =>
                      repo.updateTransaction(
                        tenantId.value,
                        txIdLong.toString,
                        new Document("cumulatedPrice", r.cumulatedPrice).append("pricingCurrency", r.currency)
                      )
                    }
                    .tapError(err =>
                      ZIO.logWarning(s"[Processor] PriceConsumption failed for tx $txIdLong: ${err.getMessage}")
                    )
                    .ignore
          }.ignore
    yield ()

  // ── TransactionEvent (OCPP 2.x) ──────────────────────────────────────────

  def handleTransactionEvent(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor    = json.hcursor.downField("payload")
    val eventType = cursor.downField("eventType").as[String].getOrElse("Updated") // Started|Updated|Ended
    val txInfo    = cursor.downField("transactionInfo")
    val txId      = txInfo.downField("transactionId").as[String].getOrElse(java.util.UUID.randomUUID().toString)
    val seqNo     = cursor.downField("seqNo").as[Int].getOrElse(0)
    val meterValue = cursor.downField("meterValue").downArray
      .downField("sampledValue").downArray
      .downField("value").as[String].map(_.toDoubleOption.getOrElse(0.0))
      .getOrElse(0.0)
    val now = java.util.Date.from(Instant.now())

    eventType match
      case "Started" =>
        val connectorId = cursor.downField("evse").downField("connectorId").as[Int].getOrElse(1)
        val idToken     = cursor.downField("idToken").downField("idToken").as[String].toOption
        for
          authResp <- authClient
            .resolveOcppAuthorization(tenantId.value, idToken.getOrElse(""), stationId.value)
            .tapError(err => ZIO.logWarning(s"[Processor] Auth lookup failed for tx $txId: ${err.getMessage}"))
            .option
          resolvedUserId = authResp.filter(_.status == "Accepted")
            .map(_.userId).filter(_.nonEmpty).orElse(idToken)
          stationDoc <- repo.getStation(tenantId.value, stationId.value)
          (connectorType, connectorPowerKw) = extractConnectorInfo(stationDoc, connectorId)
          pricingOpt <- pricingClient
            .resolvePricing(
              tenantId.value,
              stationId.value,
              connectorId.toString,
              connectorType,
              connectorPowerKw,
              resolvedUserId.getOrElse(""),
              now.getTime
            )
            .tapError(err => ZIO.logWarning(s"[Processor] ResolvePricing failed for tx $txId: ${err.getMessage}"))
            .option
          doc = new Document("_id", txId)
            .append("tenantId", tenantId.value)
            .append("chargingStationId", stationId.value)
            .append("connectorId", connectorId)
            .append("tagId", idToken.orNull)
            .append("userId", resolvedUserId.orNull)
            .append("connectorType", connectorType)
            .append("connectorPowerKw", connectorPowerKw)
            .append("meterStart", meterValue)
            .append("pricingModelJson", pricingOpt.filter(_.found).map(_.pricingModelJson).orNull)
            .append("pricingCurrency", pricingOpt.filter(_.found).map(_.currency).orNull)
            .append("timestamp", now).append("startDate", now)
            .append("inProgress", true)
            .append("createdOn", now).append("lastChangedOn", now)
          _ <- repo.createTransaction(tenantId.value, doc)
          _ <- publishTransactionLifecycle(
            tenantId,
            stationId,
            txId,
            "Start",
            connectorId,
            resolvedUserId,
            meterValue,
            now
          )
          _ <- publishAuditLog(tenantId, stationId, "TransactionEvent", s"Tx $txId started")
        yield ()

      case "Ended" =>
        val reason = cursor.downField("reason").as[String].getOrElse("Other")
        val closeDoc = new Document()
          .append("endDate", now).append("meterStop", meterValue)
          .append("stopReason", reason).append("lastChangedOn", now)
        for
          _        <- repo.closeTransaction(tenantId.value, txId, closeDoc)
          txDocOpt <- repo.getTransaction(tenantId.value, txId)
          (meterStartWh, startEpochMs, userId, pricingModelJson, connectorType, connectorPowerKw, connectorId) =
            txDocOpt match
              case None => (0.0, now.getTime, None, None, "", 0.0, 1)
              case Some(d) =>
                val ms  = Option(d.get("meterStart")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
                val sd  = Option(d.getDate("startDate")).map(_.getTime).getOrElse(now.getTime)
                val uid = Option(d.getString("userId")).orElse(Option(d.getString("tagId")))
                val pmj = Option(d.getString("pricingModelJson")).filter(_.nonEmpty)
                val ct  = Option(d.getString("connectorType")).getOrElse("")
                val cpk = Option(d.get("connectorPowerKw")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
                val cid = Option(d.get("connectorId")).flatMap(_.toString.toIntOption).getOrElse(1)
                (ms, sd, uid, pmj, ct, cpk, cid)
          consumptionWh = math.max(0.0, meterValue - meterStartWh)
          durationSecs  = math.max(0L, (now.getTime - startEpochMs) / 1000)
          finalPriceOpt <- pricingModelJson match
            case None => ZIO.succeed(None)
            case Some(pmj) =>
              pricingClient
                .finalisePrice(
                  tenantId.value,
                  txId,
                  pmj,
                  consumptionWh,
                  connectorType,
                  connectorPowerKw,
                  startEpochMs,
                  now.getTime
                )
                .map(Some(_))
                .tapError(err =>
                  ZIO.logWarning(s"[Processor] FinalisePrice failed for tx $txId: ${err.getMessage}")
                )
                .option
                .map(_.flatten)
          _ <- finalPriceOpt.fold(ZIO.unit) { fp =>
            repo.updateTransaction(
              tenantId.value,
              txId,
              new Document("totalCost", fp.totalPrice)
                .append("pricingCurrency", fp.currency)
                .append("invoiceItemJson", fp.invoiceItem)
            )
          }
          _ <- publishTransactionLifecycleEnded(
            tenantId,
            stationId,
            txId,
            connectorId,
            reason,
            meterStartWh,
            meterValue,
            startEpochMs,
            now.getTime,
            consumptionWh,
            durationSecs,
            userId,
            finalPriceOpt
          )
          _ <- publishAuditLog(tenantId, stationId, "TransactionEvent", s"Tx $txId ended: $reason")
        yield ()

      case _ /* Updated */ =>
        val connectorId = cursor.downField("evse").downField("connectorId").as[Int].getOrElse(1)
        repo.updateTransaction(
          tenantId.value,
          txId,
          new Document("currentInstantWatts", meterValue).append("lastChangedOn", now)
        ) *>
          publishConsumptionIngest(tenantId, stationId, connectorId, None, meterValue, 0.0)

  // ── DiagnosticsStatusNotification (OCPP 1.6) ─────────────────────────────

  def handleDiagnosticsStatus(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val status = json.hcursor.downField("payload").downField("status").as[String].getOrElse("Uploading")
    val fields = new Document("diagnosticsStatus", status)
      .append("lastChangedOn", java.util.Date.from(Instant.now()))
    repo.updateStationFields(tenantId.value, stationId.value, fields) *>
      publishAuditLog(tenantId, stationId, "DiagnosticsStatusNotification", s"status=$status")

  // ── FirmwareStatusNotification (OCPP 1.6) ────────────────────────────────

  def handleFirmwareStatus(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val status = json.hcursor.downField("payload").downField("status").as[String].getOrElse("Downloading")
    val fields = new Document("firmwareUpdateStatus", status)
      .append("lastChangedOn", java.util.Date.from(Instant.now()))
    repo.updateStationFields(tenantId.value, stationId.value, fields) *>
      publishAuditLog(tenantId, stationId, "FirmwareStatusNotification", s"status=$status")

  // ── DataTransfer (OCPP 1.6) ───────────────────────────────────────────────

  def handleDataTransfer(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val cursor    = json.hcursor.downField("payload")
    val vendorId  = cursor.downField("vendorId").as[String].getOrElse("")
    val messageId = cursor.downField("messageId").as[String].toOption
    ZIO.logInfo(
      s"[Processor] DataTransfer from ${stationId.value}: vendor=$vendorId msg=${messageId.orNull}"
    ) *> publishAuditLog(
      tenantId,
      stationId,
      "DataTransfer",
      s"vendorId=$vendorId messageId=${messageId.getOrElse("")}"
    )

  // ── OCPP 2.1 specific actions ─────────────────────────────────────────────

  def handleDERStartStop(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val started = json.hcursor.downField("payload").downField("started").as[Boolean].getOrElse(false)
    ZIO.logInfo(s"[Processor] DER ${if started then "start" else "stop"} from ${stationId.value}") *>
      publishSmartChargingTrigger(tenantId, stationId, "Reoptimize", None)

  def handleBatterySwap(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val eventType = json.hcursor.downField("payload").downField("eventType").as[String].getOrElse("BatteryIn")
    ZIO.logInfo(s"[Processor] BatterySwap $eventType from ${stationId.value}")

  def handleAFRRSignal(json: Json, tenantId: TenantId, stationId: ChargingStationId): Task[Unit] =
    val signal = json.hcursor.downField("payload").downField("signal").as[Int].getOrElse(0)
    ZIO.logInfo(s"[Processor] AFRRSignal $signal mW from ${stationId.value}") *>
      publishSmartChargingTrigger(tenantId, stationId, "Reoptimize", None)

  // ── Connector metadata helper ─────────────────────────────────────────────

  private def extractConnectorInfo(stationDoc: Option[Document], connectorId: Int): (String, Double) =
    stationDoc.flatMap { doc =>
      Option(doc.getList("connectors", classOf[Document]))
        .flatMap(_.asScala.find(_.getInteger("connectorId", -1) == connectorId))
        .map { c =>
          val t  = Option(c.getString("connectorType")).getOrElse("")
          val kw = Option(c.get("powerKw")).map(_.toString.toDoubleOption.getOrElse(0.0)).getOrElse(0.0)
          (t, kw)
        }
    }.getOrElse(("", 0.0))

  // ── Kafka publishing helpers ──────────────────────────────────────────────

  // Billing and roaming correlate sessions by numeric transactionId. OCPP 1.6
  // ids are always numeric; OCPP 2.x station-generated ids may not be — those
  // are published as strings and skipped by consumers that require a Long.
  private def txIdJson(txId: String): Json =
    txId.toLongOption.fold(Json.fromString(txId))(Json.fromLong)

  private def publishTransactionLifecycle(
      tenantId: TenantId,
      stationId: ChargingStationId,
      txId: String,
      action: String,
      connectorId: Int,
      userId: Option[String],
      meter: Double,
      timestamp: java.util.Date
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"          -> Json.fromString(tenantId.value),
      "chargingStationId" -> Json.fromString(stationId.value),
      "transactionId"     -> txIdJson(txId),
      "action"            -> Json.fromString(action),
      "connectorId"       -> Json.fromInt(connectorId),
      "userId"            -> userId.fold(Json.Null)(Json.fromString),
      "meterValue"        -> Json.fromDouble(meter).getOrElse(Json.fromInt(0)),
      "meterStart"        -> Json.fromDouble(meter).getOrElse(Json.fromInt(0)),
      "timestamp"         -> Json.fromLong(timestamp.getTime)
    )
    producer.publish(Topics.transactionLifecycle, txId, event)

  private def publishSmartChargingTrigger(
      tenantId: TenantId,
      stationId: ChargingStationId,
      event: String,
      connectorId: Option[Int]
  ): Task[Unit] =
    repo.getStation(tenantId.value, stationId.value).flatMap { stationDoc =>
      stationDoc.flatMap(d => Option(d.get("siteAreaId")).map(_.toString)).filter(_.nonEmpty) match
        case None =>
          ZIO.logDebug(
            s"[Processor] Station ${stationId.value} has no siteAreaId — smart-charging trigger skipped"
          )
        case Some(siteAreaId) =>
          val payload = Json.obj(
            "tenantId"          -> Json.fromString(tenantId.value),
            "siteAreaId"        -> Json.fromString(siteAreaId),
            "event"             -> Json.fromString(event),
            "chargingStationId" -> Json.fromString(stationId.value),
            "connectorId"       -> connectorId.fold(Json.Null)(Json.fromInt)
          )
          producer.publish(Topics.smartChargingTriggers, siteAreaId, payload)
    }

  private def publishConsumptionIngest(
      tenantId: TenantId,
      stationId: ChargingStationId,
      connectorId: Int,
      txId: Option[Long],
      watts: Double,
      kwh: Double
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"          -> Json.fromString(tenantId.value),
      "chargingStationId" -> Json.fromString(stationId.value),
      "connectorId"       -> Json.fromInt(connectorId),
      "transactionId"     -> txId.fold(Json.Null)(Json.fromLong),
      "time"              -> Json.fromLong(java.lang.System.currentTimeMillis()),
      "instantWatts"      -> Json.fromDouble(watts).getOrElse(Json.fromInt(0)),
      "cumulatedKwh"      -> Json.fromDouble(kwh).getOrElse(Json.fromInt(0))
    )
    producer.publish(Topics.consumptionsIngest, stationId.value, event)

  private def publishTransactionLifecycleEnded(
      tenantId: TenantId,
      stationId: ChargingStationId,
      txId: String,
      connectorId: Int,
      stopReason: String,
      meterStartWh: Double,
      meterStopWh: Double,
      startEpochMs: Long,
      endEpochMs: Long,
      consumptionWh: Double,
      durationSecs: Long,
      userId: Option[String],
      finalPrice: Option[FinalisePriceResponse]
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"          -> Json.fromString(tenantId.value),
      "chargingStationId" -> Json.fromString(stationId.value),
      "transactionId"     -> txIdJson(txId),
      "action"            -> Json.fromString("Stop"),
      "connectorId"       -> Json.fromInt(connectorId),
      "stopReason"        -> Json.fromString(stopReason),
      "meterValue"        -> Json.fromDouble(meterStopWh).getOrElse(Json.fromInt(0)),
      "timestamp"         -> Json.fromLong(endEpochMs),
      "userId"            -> userId.fold(Json.Null)(Json.fromString),
      "meterStart"        -> Json.fromDouble(meterStartWh).getOrElse(Json.fromInt(0)),
      "meterStop"         -> Json.fromDouble(meterStopWh).getOrElse(Json.fromInt(0)),
      "startDate"         -> Json.fromString(Instant.ofEpochMilli(startEpochMs).toString),
      "endDate"           -> Json.fromString(Instant.ofEpochMilli(endEpochMs).toString),
      "totalDurationSecs" -> Json.fromLong(durationSecs),
      "consumptionWh"     -> Json.fromDouble(consumptionWh).getOrElse(Json.fromInt(0)),
      "priceUnit"         -> finalPrice.fold(Json.Null)(fp => Json.fromString(fp.currency.toLowerCase)),
      "totalCost" -> finalPrice.fold(Json.Null)(fp =>
        Json.fromDouble(fp.totalPrice).getOrElse(Json.Null)
      ),
      "invoiceItemJson" -> finalPrice.filter(_.invoiceItem.nonEmpty).fold(Json.Null)(fp =>
        Json.fromString(fp.invoiceItem)
      )
    )
    producer.publish(Topics.transactionLifecycle, txId, event)

  private def publishAuditLog(
      tenantId: TenantId,
      stationId: ChargingStationId,
      action: String,
      message: String
  ): Task[Unit] =
    val event = Json.obj(
      "tenantId"          -> Json.fromString(tenantId.value),
      "timestamp"         -> Json.fromLong(java.lang.System.currentTimeMillis()),
      "level"             -> Json.fromString("I"),
      "source"            -> Json.fromString("OcppProcessor"),
      "action"            -> Json.fromString(action),
      "message"           -> Json.fromString(message),
      "chargingStationId" -> Json.fromString(stationId.value)
    )
    producer.publish(Topics.auditLog, stationId.value, event)

object OcppEventProcessor:

  val live: ZLayer[
    ProcessorRepository & EvKafkaProducer & KafkaConfig & ProcessorAuthClient & ProcessorGatewayClient & ProcessorPricingClient,
    Nothing,
    OcppEventProcessor
  ] = ZLayer.fromFunction(OcppEventProcessor(_, _, _, _, _, _))
