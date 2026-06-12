package io.itbeans.ev.otel

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{StatusCode, Tracer}
import zio._

// ---------------------------------------------------------------------------
// EvTracing — ZIO service for OpenTelemetry span creation.
//
// Usage:
//   tracing.span("auth.signIn")(signIn(body, ...))
//   tracing.spanTask("pricing.resolve", "tenant" -> tenantId)(pricingCall)
//
// Errors are recorded on the span automatically.
// The noop implementation is used when OTel is disabled (otel.enabled=false).
// ---------------------------------------------------------------------------

trait EvTracing:
  def span[R, E, A](name: String, attributes: (String, String)*)(effect: ZIO[R, E, A]): ZIO[R, E, A]
  def spanTask[A](name: String, attributes: (String, String)*)(effect: Task[A]): Task[A]

object EvTracing:

  def span[R, E, A](name: String, attributes: (String, String)*)(
      effect: ZIO[R, E, A]
  ): ZIO[R & EvTracing, E, A] =
    ZIO.serviceWithZIO[EvTracing](_.span(name, attributes*)(effect))

  val noop: ULayer[EvTracing] = ZLayer.succeed {
    new EvTracing:
      def span[R, E, A](name: String, attributes: (String, String)*)(effect: ZIO[R, E, A]): ZIO[R, E, A] = effect
      def spanTask[A](name: String, attributes: (String, String)*)(effect: Task[A]): Task[A]             = effect
  }

  val live: ZLayer[OpenTelemetry, Nothing, EvTracing] =
    ZLayer.fromFunction { (otel: OpenTelemetry) =>
      new LiveEvTracing(otel.getTracer("ev-service"))
    }

final private class LiveEvTracing(tracer: Tracer) extends EvTracing:

  private def withSpan[R, E, A](name: String, attributes: Seq[(String, String)])(
      effect: ZIO[R, E, A]
  ): ZIO[R, E, A] =
    ZIO.acquireReleaseWith(
      ZIO.succeed {
        val builder = tracer.spanBuilder(name)
        attributes.foreach { case (k, v) => builder.setAttribute(k, v) }
        val span  = builder.startSpan()
        val scope = span.makeCurrent()
        (span, scope)
      }
    ) { case (span, scope) =>
      ZIO.succeed { scope.close(); span.end() }
    } { case (span, _) =>
      effect.tapError(e =>
        ZIO.succeed(e match
          case t: Throwable =>
            span.setStatus(StatusCode.ERROR, t.getMessage)
            span.recordException(t)
          case other =>
            span.setStatus(StatusCode.ERROR, other.toString)
        )
      )
    }

  def span[R, E, A](name: String, attributes: (String, String)*)(effect: ZIO[R, E, A]): ZIO[R, E, A] =
    withSpan(name, attributes)(effect)

  def spanTask[A](name: String, attributes: (String, String)*)(effect: Task[A]): Task[A] =
    withSpan(name, attributes)(effect)
