package io.itbeans.ev.ocppgateway

import io.itbeans.ev.domain.{ChargingStationId, OcppVersion, TenantId}
import zio._
import zio.http.WebSocketChannel
import zio.metrics.Metric
import zio.stream.ZStream

// ---------------------------------------------------------------------------
// ConnectionRegistry — thread-safe registry of live OCPP WebSocket connections.
//
// Each charging station is a key in a Ref[Map].
// ZHub[OcppConnectionEvent] broadcasts state changes to interested consumers
// (smart charging, monitoring, etc.).
// ---------------------------------------------------------------------------

case class ConnectionKey(tenantId: TenantId, stationId: ChargingStationId)

case class ConnectionEntry(
    key: ConnectionKey,
    channel: WebSocketChannel,
    ocppVersion: OcppVersion,
    connectedAt: java.time.Instant
)

sealed trait OcppConnectionEvent
case class StationConnected(key: ConnectionKey, version: OcppVersion) extends OcppConnectionEvent
case class StationDisconnected(key: ConnectionKey)                    extends OcppConnectionEvent

trait ConnectionRegistry:
  def register(entry: ConnectionEntry): UIO[Unit]
  def deregister(key: ConnectionKey): UIO[Unit]
  def get(key: ConnectionKey): UIO[Option[ConnectionEntry]]
  def listAll(tenantId: Option[TenantId]): UIO[List[ConnectionEntry]]
  def events: ZStream[Any, Nothing, OcppConnectionEvent]

object ConnectionRegistry:

  private val connectedStations = Metric.gauge("ocpp_connected_stations")

  val live: ZLayer[Any, Nothing, ConnectionRegistry] =
    ZLayer.fromZIO {
      for
        connections <- Ref.make(Map.empty[ConnectionKey, ConnectionEntry])
        hub         <- Hub.bounded[OcppConnectionEvent](256)
      yield new ConnectionRegistry:
        def register(entry: ConnectionEntry): UIO[Unit] =
          connections.update(_ + (entry.key -> entry)) *>
            connectedStations.increment *>
            hub.publish(StationConnected(entry.key, entry.ocppVersion)).unit

        def deregister(key: ConnectionKey): UIO[Unit] =
          connections.update(_ - key) *>
            connectedStations.decrement *>
            hub.publish(StationDisconnected(key)).unit

        def get(key: ConnectionKey): UIO[Option[ConnectionEntry]] =
          connections.get.map(_.get(key))

        def listAll(tenantId: Option[TenantId]): UIO[List[ConnectionEntry]] =
          connections.get.map { m =>
            tenantId.fold(m.values.toList)(tid => m.values.filter(_.key.tenantId == tid).toList)
          }

        def events: ZStream[Any, Nothing, OcppConnectionEvent] =
          ZStream.fromHub(hub)
    }
