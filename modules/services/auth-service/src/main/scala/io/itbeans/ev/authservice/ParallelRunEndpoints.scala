package io.itbeans.ev.authservice

import io.circe.{Decoder, Encoder}
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// ---------------------------------------------------------------------------
// ParallelRunEndpoints — Tapir endpoint definitions for the parallel-run
// comparison harness.
//
//   POST /auth/parallel-run/compare
//     Body:    { "tenant": "...", "email": "...", "tsToken": "..." }
//     Returns: { "matched": bool, "diffCount": int, "diffs": {...} }
//
//   GET /auth/parallel-run/report?tenant=...&fromMs=...&toMs=...
//     Returns: ParallelRunReport aggregate statistics
//
// Both endpoints are only registered when AuthConfig.parallelRunEnabled = true.
// The TypeScript monolith calls /compare after each successful login.
// The ops team polls /report to monitor parity before the Kong cutover.
// ---------------------------------------------------------------------------

private given Schema[Instant] = Schema.string[Instant]

object ParallelRunEndpoints:

  private val base =
    endpoint.in("auth" / "parallel-run").errorOut(stringBody)

  val compare =
    base.post
      .in("compare")
      .in(jsonBody[ParallelRunCompareRequest])
      .out(jsonBody[ParallelRunCompareResponse])

  val report =
    base.get
      .in("report")
      .in(query[String]("tenant"))
      .in(query[Long]("fromMs"))
      .in(query[Long]("toMs"))
      .out(jsonBody[ParallelRunReport])
