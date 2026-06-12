package io.itbeans.ev.authservice

import io.circe.Json
import io.circe.syntax._
import io.itbeans.ev.domain.TenantId
import zio._

import java.time.Instant
import java.util.UUID

import CirceInstances.given

// ---------------------------------------------------------------------------
// ParallelRunService — token comparison logic for the 60-day parallel run.
//
// Flow for each compare call:
//   1. Validate the TypeScript JWT (shared HS256 secret) — rejects forged tokens
//   2. Decode its UserToken payload
//   3. Look up the user + tenant in MongoDB
//   4. Issue a Scala JWT via TokenService
//   5. Decode the Scala JWT payload
//   6. Diff every JSON field in both payloads, excluding time-varying ones
//   7. Persist the ParallelRunComparison record
//   8. Return the comparison summary to the caller
//
// Fields excluded from diff (legitimately differ per issuance):
//   iat, exp — JWT standard claims (set per-call)
//
// The persisted record includes the Scala token for offline debugging;
// the TypeScript token is NOT stored (it was already in the TS monolith's logs).
// ---------------------------------------------------------------------------

final class ParallelRunService(
    tokenSvc: TokenService,
    userRepo: UserRepository,
    tenantRepo: TenantRepository,
    repo: ParallelRunRepository,
    cfg: AuthConfig
):

  // Fields that legitimately differ per JWT issuance and must not be compared.
  private val ignoredFields = Set("iat", "exp")

  def compare(
      tenant: String,
      email: String,
      tsToken: String
  ): Task[ParallelRunComparison] =
    for
      // 1 + 2. Validate TypeScript JWT and decode its payload.
      tsUserToken <- tokenSvc.validateToken(tsToken)
        .mapError(e => new Exception(s"Invalid TypeScript token: ${e.getMessage}"))

      // 3. Resolve tenant + user from MongoDB.
      authTenant <- resolveTenant(tenant)
      tenantId = TenantId(authTenant.id)
      authUser <- userRepo.findByEmail(tenantId, email).flatMap {
        case None    => ZIO.fail(new Exception(s"User '$email' not found in tenant '${authTenant.id}'"))
        case Some(u) => ZIO.succeed(u)
      }

      // 4. Issue Scala JWT.
      scalaRawToken <- tokenSvc.issueToken(tenantId, authUser, authTenant)

      // 5. Decode Scala JWT payload.
      scalaUserToken <- tokenSvc.validateToken(scalaRawToken)

      // 6. Diff the two payloads field-by-field.
      diffs = diffTokens(tsUserToken, scalaUserToken)

      now = Instant.now()
      record = ParallelRunComparison(
        id = UUID.randomUUID().toString,
        tenantId = authTenant.id,
        userEmail = email,
        timestamp = now,
        matched = diffs.isEmpty,
        diffCount = diffs.size,
        diffs = diffs,
        scalaToken = scalaRawToken
      )

      // 7. Persist (fire-and-forget — don't fail the response if storage fails).
      _ <- repo.save(record)
        .tapError(e => ZIO.logWarning(s"[ParallelRun] Failed to save comparison record: ${e.getMessage}"))
        .ignore
    yield record

  def buildReport(tenantId: String, from: Instant, to: Instant): Task[ParallelRunReport] =
    for
      total   <- repo.countInRange(tenantId, from, to)
      matched <- repo.countMatchedInRange(tenantId, from, to)
      // Load all records to compute per-field divergence counts (parallel run volume is low)
      records <- repo.findInRange(tenantId, from, to)
      fieldCounts = records
        .flatMap(_.diffs.keys)
        .groupBy(identity)
        .view.mapValues(_.size.toLong)
        .toMap
      matchRate = if total == 0 then 100.0 else matched.toDouble / total.toDouble * 100.0
    yield ParallelRunReport(
      tenantId = tenantId,
      periodFrom = from,
      periodTo = to,
      totalComparisons = total,
      matchedCount = matched,
      matchRatePct = math.round(matchRate * 100.0) / 100.0,
      divergentFieldCounts = fieldCounts
    )

  // ── Diff logic ────────────────────────────────────────────────────────────

  private def diffTokens(ts: UserToken, scala: UserToken): Map[String, FieldDiff] =
    val tsJson    = ts.asJson.deepDropNullValues
    val scalaJson = scala.asJson.deepDropNullValues
    diffJsonObjects(tsJson, scalaJson)

  private def diffJsonObjects(ts: Json, sc: Json): Map[String, FieldDiff] =
    val tsObj   = ts.asObject.getOrElse(io.circe.JsonObject.empty)
    val scObj   = sc.asObject.getOrElse(io.circe.JsonObject.empty)
    val allKeys = (tsObj.keys ++ scObj.keys).toSet -- ignoredFields
    allKeys.flatMap { key =>
      val tsVal = tsObj(key).map(_.noSpaces).getOrElse("<absent>")
      val scVal = scObj(key).map(_.noSpaces).getOrElse("<absent>")
      if tsVal == scVal then None
      else Some(key -> FieldDiff(tsValue = tsVal, scalaValue = scVal))
    }.toMap

  // ── Tenant resolution ─────────────────────────────────────────────────────

  private def resolveTenant(subdomain: String): Task[AuthTenant] =
    if subdomain.isBlank then
      tenantRepo.findById(cfg.defaultTenantId).flatMap {
        case None    => ZIO.fail(new Exception(s"Default tenant '${cfg.defaultTenantId}' not found"))
        case Some(t) => ZIO.succeed(t)
      }
    else
      tenantRepo.findBySubdomain(subdomain).flatMap {
        case None    => ZIO.fail(new Exception(s"Tenant '$subdomain' not found"))
        case Some(t) => ZIO.succeed(t)
      }

object ParallelRunService:

  val live: ZLayer[
    TokenService & UserRepository & TenantRepository & ParallelRunRepository & AuthConfig,
    Nothing,
    ParallelRunService
  ] =
    ZLayer.fromFunction(new ParallelRunService(_, _, _, _, _))
