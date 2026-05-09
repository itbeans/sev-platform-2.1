package io.itbeans.ev.pricingservice

import io.itbeans.ev.domain.TenantId
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// TariffResolver — selects the applicable PricingDefinition for a session.
//
// Replicates the TypeScript `PricingEngine.resolvePricingContext` logic:
//
//   Hierarchy (most → least specific):
//     ChargingStation → SiteArea → Site → Company → Tenant
//
//   For each level, static restrictions are checked:
//     1. Date range: validFrom ≤ now < validTo (if set)
//     2. Connector type: must match (if set)
//     3. Connector power (kW): must match within tolerance (if set)
//
//   The first tariff that passes static restrictions wins.
//   Falls back to None if no tariff matches (caller creates a zero-price model).
// ---------------------------------------------------------------------------

final class TariffResolver(repo: PricingRepository):

  /**
   * Resolve the applicable pricing definition for a new charging session.
   * Returns None if no tariff matches (zero-price session).
   */
  def resolve(ctx: TariffResolver.ResolutionContext): Task[Option[PricingDefinition]] =
    for
      // Load candidates at each hierarchy level (most specific first)
      stationDefs <- repo.findByEntity(ctx.tenantId, PricingEntity.ChargingStation, ctx.chargingStationId)
      siteAreaDefs <- ctx.siteAreaId.fold(ZIO.succeed(List.empty[PricingDefinition]))(id =>
        repo.findByEntity(ctx.tenantId, PricingEntity.SiteArea, id)
      )
      siteDefs <- ctx.siteId.fold(ZIO.succeed(List.empty[PricingDefinition]))(id =>
        repo.findByEntity(ctx.tenantId, PricingEntity.Site, id)
      )
      companyDefs <- ctx.companyId.fold(ZIO.succeed(List.empty[PricingDefinition]))(id =>
        repo.findByEntity(ctx.tenantId, PricingEntity.Company, id)
      )
      tenantDefs <- repo.findByTenant(ctx.tenantId)
      // Select the first match across the hierarchy
      candidates = stationDefs ++ siteAreaDefs ++ siteDefs ++ companyDefs ++ tenantDefs
      result     = candidates.find(d => passesStaticRestrictions(d, ctx))
    yield result

  // ── Static restriction checks ────────────────────────────────────────────

  private def passesStaticRestrictions(d: PricingDefinition, ctx: TariffResolver.ResolutionContext): Boolean =
    d.staticRestrictions.forall(r =>
      checkDateValidity(r, ctx.timestamp) &&
        checkConnectorType(r, ctx.connectorType) &&
        checkConnectorPower(r, ctx.connectorPowerKw)
    )

  private def checkDateValidity(r: PricingStaticRestriction, now: Instant): Boolean =
    r.validFrom.forall(from => !now.isBefore(from)) &&
      r.validTo.forall(to => now.isBefore(to))

  private def checkConnectorType(r: PricingStaticRestriction, connectorType: String): Boolean =
    r.connectorType.forall(required => required.equalsIgnoreCase(connectorType))

  /** Match connector power within ±0.1 kW tolerance. */
  private def checkConnectorPower(r: PricingStaticRestriction, powerKw: Double): Boolean =
    r.connectorPowerKw.forall(required => math.abs(required - powerKw) < 0.1)

object TariffResolver:

  /** Context provided at session start for tariff resolution. */
  case class ResolutionContext(
      chargingStationId: String,
      siteAreaId: Option[String],
      siteId: Option[String],
      companyId: Option[String],
      tenantId: TenantId,
      connectorType: String,
      connectorPowerKw: Double,
      timestamp: Instant
  )

  val live: ZLayer[PricingRepository, Nothing, TariffResolver] =
    ZLayer.fromFunction(new TariffResolver(_))
