package io.itbeans.ev.smartcharging

import _root_.io.grpc.netty.NettyServerBuilder
import _root_.io.itbeans.ev.smartcharging.grpc.smart_charging_service.{
  BuildChargingProfilesRequest => ProtoBuildRequest,
  BuildChargingProfilesResponse => ProtoBuildResponse,
  ChargingProfileResult => ProtoChargingProfileResult,
  CheckConnectionRequest => ProtoCheckConnectionRequest,
  CheckConnectionResponse => ProtoCheckConnectionResponse,
  SmartChargingServiceGrpc,
  TriggerSmartChargingRequest => ProtoTriggerRequest,
  TriggerSmartChargingResponse => ProtoTriggerResponse
}
import io.itbeans.ev.otel.EvTracing
import zio._

import scala.concurrent.{ExecutionContext, Future}

// ---------------------------------------------------------------------------
// SmartChargingGrpcTransport — bridges ScalaPB-generated Future-based gRPC
// service trait to the ZIO-native SmartChargingGrpcHandler.
//
// Three RPCs match smart_charging_service.proto:
//   TriggerSmartCharging  — compute + apply profiles, returns counts
//   CheckConnection       — probe the SAP optimizer endpoint
//   BuildChargingProfiles — dry-run compute without delivery
// ---------------------------------------------------------------------------

final class SmartChargingGrpcTransport(
    handler: SmartChargingGrpcHandler,
    tracing: EvTracing,
    rt: Runtime[Any]
) extends SmartChargingServiceGrpc.SmartChargingService:

  private def run[A](spanName: String)(effect: Task[A]): Future[A] =
    Unsafe.unsafe(implicit u => rt.unsafe.runToFuture(tracing.spanTask(spanName)(effect)))

  override def triggerSmartCharging(req: ProtoTriggerRequest): Future[ProtoTriggerResponse] =
    run("smartcharging.triggerSmartCharging")(
      handler
        .triggerSmartCharging(TriggerSmartChargingRequestADT(
          tenantId = req.tenantId,
          siteAreaId = req.siteAreaId,
          retry = req.retry
        ))
        .map(r =>
          ProtoTriggerResponse(
            success = r.success,
            profilesApplied = r.profilesApplied,
            profilesFailed = r.profilesFailed,
            error = r.error
          )
        )
    )

  override def checkConnection(req: ProtoCheckConnectionRequest): Future[ProtoCheckConnectionResponse] =
    run("smartcharging.checkConnection")(
      handler
        .checkConnection(CheckConnectionRequestADT(tenantId = req.tenantId))
        .map(r => ProtoCheckConnectionResponse(connected = r.connected, error = r.error))
    )

  override def buildChargingProfiles(req: ProtoBuildRequest): Future[ProtoBuildResponse] =
    run("smartcharging.buildChargingProfiles")(
      handler
        .buildChargingProfiles(BuildChargingProfilesRequestADT(
          tenantId = req.tenantId,
          siteAreaId = req.siteAreaId,
          excludedStationIds = req.excludedStationIds.toList
        ))
        .map(r =>
          ProtoBuildResponse(
            profiles = r.profiles.map(p =>
              ProtoChargingProfileResult(
                chargingStationId = p.chargingStationId,
                connectorId = p.connectorId,
                chargingProfileJson = p.chargingProfileJson,
                limitWatts = p.limitWatts
              )
            ),
            error = r.error
          )
        )
    )

object SmartChargingGrpcTransport:

  val start: RIO[SmartChargingGrpcHandler & SmartChargingConfig & EvTracing, Unit] =
    for
      handler <- ZIO.service[SmartChargingGrpcHandler]
      cfg     <- ZIO.service[SmartChargingConfig]
      tracing <- ZIO.service[EvTracing]
      rt      <- ZIO.runtime[Any]
      impl = new SmartChargingGrpcTransport(handler, tracing, rt)
      server <- ZIO.attempt(
        NettyServerBuilder
          .forPort(cfg.grpcPort)
          .addService(SmartChargingServiceGrpc.bindService(impl, ExecutionContext.global))
          .build()
          .start()
      )
      _ <- ZIO.logInfo(s"[SmartCharging] gRPC server listening on :${server.getPort}")
    yield ()
