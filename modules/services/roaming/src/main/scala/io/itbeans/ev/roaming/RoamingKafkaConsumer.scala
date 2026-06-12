package io.itbeans.ev.roaming

import io.circe.parser.decode
import io.itbeans.ev.domain.TenantId
import io.itbeans.ev.kafka.{KafkaConfig, Topics}
import zio._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde

// ---------------------------------------------------------------------------
// RoamingKafkaConsumer — consumes transactions.lifecycle and generates CDRs.
//
// On TransactionAction.Stop:
//   1. Read the full transaction from MongoDB
//   2. Build OCPI CDR → push to all registered EMSP partners
//   3. Build OICP CDR → push to all registered OICP partners
//
// Errors per transaction are logged and do not stop the consumer.
// Offset is committed only after successful (or logged-failure) processing.
// ---------------------------------------------------------------------------

final class RoamingKafkaConsumer(
    txRepo: TransactionReadRepository,
    stationRepo: ChargingStationLocationRepository,
    ocpiRepo: OcpiEndpointRepository,
    oicpRepo: OicpEndpointRepository,
    cdrSvc: CdrService,
    ocpiClient: OcpiCpoClient,
    oicpClient: OicpCpoClient,
    kafkaCfg: KafkaConfig
):

  def start: Task[Unit] =
    val settings = ConsumerSettings(kafkaCfg.bootstrapServers)
      .withGroupId(kafkaCfg.groupId)
      .withProperty("auto.offset.reset", kafkaCfg.autoOffsetReset)
      .withProperty("enable.auto.commit", "false")

    ZIO.scoped {
      Consumer.make(settings).flatMap { consumer =>
        consumer
          .plainStream(
            Subscription.topics(Topics.transactionLifecycle),
            Serde.string,
            Serde.string
          )
          .mapZIO { record =>
            processRecord(record.value)
              .catchAll(err =>
                ZIO.logError(s"Failed to process transaction lifecycle record: ${err.getMessage}")
              )
              .as(record.offset)
          }
          .aggregateAsync(Consumer.offsetBatches)
          .mapZIO(_.commit)
          .runDrain
      }
    }

  private def processRecord(json: String): Task[Unit] =
    for
      payload <- ZIO.fromEither(decode[TransactionLifecyclePayload](json))
        .mapError(e => new Exception(s"Invalid TransactionLifecyclePayload: $e"))
      _ <- if payload.action == "Stop" then
        handleTransactionStop(payload)
          .tapError(err =>
            ZIO.logError(
              s"CDR generation failed for tx ${payload.transactionId}: ${err.getMessage}"
            )
          )
          .ignore
      else ZIO.unit // Only process Stop events
    yield ()

  private def handleTransactionStop(payload: TransactionLifecyclePayload): Task[Unit] =
    val tenantId = TenantId(payload.tenantId)
    for
      txOpt <- txRepo.findById(tenantId, payload.transactionId)
      tx <- txOpt match
        case None    => ZIO.fail(new Exception(s"Transaction ${payload.transactionId} not found"))
        case Some(t) => ZIO.succeed(t)
      // Enrich CDR with real station address, coordinates, and connector type
      stationLoc <- stationRepo.findLocation(tenantId, tx.chargingStationId).ignore.map(_.toOption.flatten)
      ocpiPartners <- ocpiRepo.findRegistered(tenantId)
        .map(_.filter(_.role == OcpiRole.EMSP))
      oicpPartners <- oicpRepo.findRegistered(tenantId)
      // Push OCPI CDRs to all EMSP partners
      _ <- ZIO.foreachDiscard(ocpiPartners) { partner =>
        val currency = tx.currency.getOrElse("EUR")
        val cdr      = cdrSvc.buildOcpiCdr(tx, currency, stationLoc)
        ocpiClient.pushCdr(partner, cdr)
          .tapError(err =>
            ZIO.logWarning(
              s"OCPI CDR push to ${partner.id} failed: ${err.getMessage}"
            )
          )
          .ignore
      }
      // Push OICP CDRs to all OICP partners
      _ <- ZIO.foreachDiscard(oicpPartners) { partner =>
        val cdr = cdrSvc.buildOicpCdr(tx, partner.operatorId)
        oicpClient.pushCdr(partner, cdr)
          .tapError(err =>
            ZIO.logWarning(
              s"OICP CDR push to ${partner.id} failed: ${err.getMessage}"
            )
          )
          .ignore
      }
      _ <- ZIO.logInfo(
        s"CDR processing complete for tx ${payload.transactionId}: " +
          s"${ocpiPartners.length} OCPI + ${oicpPartners.length} OICP partners"
      )
    yield ()

object RoamingKafkaConsumer:

  val live: ZLayer[
    TransactionReadRepository & ChargingStationLocationRepository &
      OcpiEndpointRepository & OicpEndpointRepository &
      CdrService & OcpiCpoClient & OicpCpoClient & KafkaConfig,
    Nothing,
    RoamingKafkaConsumer
  ] =
    ZLayer.fromFunction(new RoamingKafkaConsumer(_, _, _, _, _, _, _, _))
