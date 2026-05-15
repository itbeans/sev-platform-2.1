package io.itbeans.ev.otel

import zio._

// ---------------------------------------------------------------------------
// OtelConfig — ZIO Config derivation for OpenTelemetry.
//
// All fields have safe defaults so services work without explicit config.
// Enable in production by setting otel.enabled=true + otel.otlpEndpoint.
// ---------------------------------------------------------------------------

case class OtelConfig(
    otlpEndpoint: String,
    serviceName: String,
    enabled: Boolean
)

object OtelConfig:

  given Config[OtelConfig] = (
    Config.string("otlpEndpoint").withDefault("http://jaeger:4317") zip
      Config.string("serviceName").withDefault("ev-service") zip
      Config.boolean("enabled").withDefault(false)
  ).nested("otel").map { case (ep, sn, en) => OtelConfig(ep, sn, en) }
