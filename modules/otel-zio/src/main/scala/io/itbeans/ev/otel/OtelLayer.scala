package io.itbeans.ev.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor
import zio._

// ---------------------------------------------------------------------------
// OtelLayer — builds the OpenTelemetry SDK and manages its lifecycle.
//
// When otel.enabled=false the layer returns OpenTelemetry.noop() so spans
// are created but immediately discarded, with zero performance overhead.
// ---------------------------------------------------------------------------

object OtelLayer:

  val live: ZLayer[OtelConfig, Nothing, OpenTelemetry] =
    ZLayer.scoped {
      ZIO.service[OtelConfig].flatMap { cfg =>
        if !cfg.enabled then ZIO.succeed(OpenTelemetry.noop())
        else
          ZIO.acquireRelease(
            ZIO.attempt {
              val resource = Resource.getDefault.merge(
                Resource.create(
                  Attributes.of(AttributeKey.stringKey("service.name"), cfg.serviceName)
                )
              )
              val exporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(cfg.otlpEndpoint)
                .build()
              val processor = BatchSpanProcessor.builder(exporter).build()
              val tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(processor)
                .build()
              OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build()
            }.orDie
          )(sdk => ZIO.succeed(sdk.getSdkTracerProvider.shutdown()))
      }
    }
