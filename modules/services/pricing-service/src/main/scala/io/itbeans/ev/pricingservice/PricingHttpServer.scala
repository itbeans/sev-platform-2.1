package io.itbeans.ev.pricingservice

import io.itbeans.ev.domain.TenantId
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import sttp.tapir.ztapir._
import zio._
import zio.http.{Response, Routes}

import java.time.Instant
import java.util.UUID

// ---------------------------------------------------------------------------
// PricingHttpServer — wires Tapir endpoints to ZIO HTTP routes.
//
// All handlers delegate to PricingRepository (CRUD) for tariff management.
// The gRPC endpoints (ResolvePricing, PriceConsumption, FinalisePrice) are
// served separately via PricingGrpcHandler.
// ---------------------------------------------------------------------------

object PricingHttpServer:

  def routes(repo: PricingRepository): Routes[Any, Response] =
    val endpoints: List[ZServerEndpoint[Any, ZioStreams & WebSockets]] = List(
      PricingEndpoints.listPricing.zServerLogic { case (tenantId, limitQ, skipQ) =>
        val tid = TenantId(tenantId)
        (for
          total <- repo.countAll(tid)
          items <- repo.findAll(tid, limitQ.getOrElse(100), skipQ.getOrElse(0))
        yield PricingListResponse(total, items))
          .mapError(_.getMessage)
      },
      PricingEndpoints.createPricing.zServerLogic { case (tenantId, body) =>
        val tid = TenantId(tenantId)
        val now = Instant.now()
        val definition = PricingDefinition(
          id = UUID.randomUUID().toString,
          entityId = body.entityId,
          entityType = body.entityType,
          name = body.name,
          description = body.description,
          siteId = body.siteId,
          dimensions = body.dimensions,
          staticRestrictions = body.staticRestrictions,
          restrictions = body.restrictions,
          currencyCode = body.currencyCode,
          createdOn = now,
          lastChangedOn = now
        )
        repo.upsert(tid, definition).as(definition).mapError(_.getMessage)
      },
      PricingEndpoints.getPricing.zServerLogic { case (id, tenantId) =>
        repo.findById(TenantId(tenantId), id)
          .flatMap {
            case Some(d) => ZIO.succeed(d)
            case None    => ZIO.fail(new Exception(s"PricingDefinition $id not found"))
          }
          .mapError(_.getMessage)
      },
      PricingEndpoints.updatePricing.zServerLogic { case (id, tenantId, body) =>
        val tid = TenantId(tenantId)
        (for
          existing <- repo.findById(tid, id).flatMap {
            case Some(d) => ZIO.succeed(d)
            case None    => ZIO.fail(new Exception(s"PricingDefinition $id not found"))
          }
          updated = existing.copy(
            name = body.name.getOrElse(existing.name),
            description = body.description.orElse(existing.description),
            dimensions = body.dimensions.getOrElse(existing.dimensions),
            staticRestrictions = body.staticRestrictions.orElse(existing.staticRestrictions),
            restrictions = body.restrictions.orElse(existing.restrictions),
            currencyCode = body.currencyCode.getOrElse(existing.currencyCode),
            lastChangedOn = Instant.now()
          )
          _ <- repo.upsert(tid, updated)
        yield updated)
          .mapError(_.getMessage)
      },
      PricingEndpoints.deletePricing.zServerLogic { case (id, tenantId) =>
        repo.delete(TenantId(tenantId), id).mapError(_.getMessage)
      }
    )

    ZioHttpInterpreter().toHttp(endpoints)
