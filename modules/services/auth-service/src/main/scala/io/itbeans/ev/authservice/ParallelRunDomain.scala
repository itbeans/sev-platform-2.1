package io.itbeans.ev.authservice

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

import java.time.Instant

import CirceInstances.given

// ---------------------------------------------------------------------------
// ParallelRunDomain — types for the 60-day parallel-run comparison harness.
//
// During the migration window the TypeScript monolith serves all real traffic.
// After each successful login the monolith POSTs its issued JWT to Scala's
// /auth/parallel-run/compare endpoint.  The Scala service:
//   1. Validates the TypeScript JWT (shared secret → proves authenticity)
//   2. Re-issues its own JWT for the same user
//   3. Diffs all payload fields
//   4. Persists the result to {tenantId}.parallelruncomparisons
//
// The /auth/parallel-run/report endpoint surfaces aggregate parity statistics
// so we can confirm 100% field-level match before the Kong cutover.
// ---------------------------------------------------------------------------

/** One field that differs between the two token payloads. */
case class FieldDiff(tsValue: String, scalaValue: String)

object FieldDiff:
  given Encoder[FieldDiff] = deriveEncoder
  given Decoder[FieldDiff] = deriveDecoder

/** Stored result of one TypeScript ↔ Scala token comparison. */
case class ParallelRunComparison(
    id: String,
    tenantId: String,
    userEmail: String,
    timestamp: Instant,
    matched: Boolean, // true when diffs is empty
    diffCount: Int,
    diffs: Map[String, FieldDiff], // field name → (tsValue, scalaValue)
    scalaToken: String             // Scala JWT stored for debugging
)

object ParallelRunComparison:
  given Encoder[ParallelRunComparison] = deriveEncoder
  given Decoder[ParallelRunComparison] = deriveDecoder

/** Input to POST /auth/parallel-run/compare */
case class ParallelRunCompareRequest(
    tenant: String, // tenant subdomain (empty = super-admin)
    email: String,
    tsToken: String // JWT issued by the TypeScript monolith
)

object ParallelRunCompareRequest:
  given Encoder[ParallelRunCompareRequest] = deriveEncoder
  given Decoder[ParallelRunCompareRequest] = deriveDecoder

/** Response from POST /auth/parallel-run/compare */
case class ParallelRunCompareResponse(
    matched: Boolean,
    diffCount: Int,
    diffs: Map[String, FieldDiff]
)

object ParallelRunCompareResponse:
  given Encoder[ParallelRunCompareResponse] = deriveEncoder
  given Decoder[ParallelRunCompareResponse] = deriveDecoder

/** Aggregate parity statistics for GET /auth/parallel-run/report */
case class ParallelRunReport(
    tenantId: String,
    periodFrom: Instant,
    periodTo: Instant,
    totalComparisons: Long,
    matchedCount: Long,
    matchRatePct: Double,
    divergentFieldCounts: Map[String, Long] // field → number of comparisons where it diverged
)

object ParallelRunReport:
  given Encoder[ParallelRunReport] = deriveEncoder
  given Decoder[ParallelRunReport] = deriveDecoder
