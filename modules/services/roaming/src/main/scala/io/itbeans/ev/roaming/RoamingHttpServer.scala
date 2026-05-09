package io.itbeans.ev.roaming

import io.circe.syntax._
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
// RoamingHttpServer — wires Tapir roaming endpoints to ZIO HTTP routes.
//
// OCPI credential exchange (CPO role):
//   A new EMSP partner POSTs credentials → we store them as OcpiEndpoint.
//   We return our own credentials so the partner can configure the reverse.
//
// CDR receive (EMSP role):
//   A CPO pushes a CDR to our EMSP endpoint → we store it for billing/analytics.
//
// Management:
//   Admin tooling registers/lists partners via /ocpi/mgmt/endpoints.
// ---------------------------------------------------------------------------

object RoamingHttpServer:

  def routes(
      ocpiRepo: OcpiEndpointRepository,
      cfg: RoamingConfig
  ): Routes[Any, Response] =
    val endpoints: List[ZServerEndpoint[Any, ZioStreams & WebSockets]] = List(
      // ── CPO credentials GET ────────────────────────────────────────────────

      RoamingEndpoints.getCpoCredentials.zServerLogic { case authHeader =>
        // Return our CPO credentials — the token in the header identifies the partner.
        // In a full implementation we'd look up the partner to personalise the response.
        validateOcpiToken(authHeader).as {
          OcpiResponse.success(ourCpoCredentials(cfg))
        }.mapError(_.getMessage)
      },

      // ── CPO credentials POST (new partner registration) ────────────────────

      RoamingEndpoints.postCpoCredentials.zServerLogic { case (authHeader, body) =>
        val now = Instant.now()
        (for
          _ <- validateOcpiToken(authHeader)
          // Use the token they sent as their localToken (we'll include it in outbound calls).
          // Generate a new local token for them to use when calling us.
          localToken = UUID.randomUUID().toString.replace("-", "")
          ep = OcpiEndpoint(
            id = UUID.randomUUID().toString,
            tenantId = cfg.defaultTenantId,
            countryCode = body.roles.headOption.map(_.country_code).getOrElse(""),
            partyId = body.roles.headOption.map(_.party_id).getOrElse(""),
            role = OcpiRole.EMSP,
            name = body.roles.headOption.map(_.business_details.name).getOrElse("Unknown"),
            baseUrl = body.url,
            token = body.token,
            localToken = localToken,
            status = OcpiEndpointStatus.Registered,
            backgroundPatchJob = false,
            locationsUrl = None,
            sessionsUrl = None,
            cdrsUrl = None,
            tariffUrl = None,
            tokensUrl = None,
            commandsUrl = None,
            lastPatchJobOn = None,
            lastPatchJobResult = None,
            createdOn = now,
            lastChangedOn = now
          )
          _ <- ocpiRepo.upsert(TenantId(cfg.defaultTenantId), ep)
          _ <- ZIO.logInfo(s"Registered OCPI EMSP partner: ${ep.name} (${ep.countryCode}-${ep.partyId})")
        yield OcpiResponse.success(ourCpoCredentials(cfg)))
          .mapError(_.getMessage)
      },

      // ── CPO credentials PUT (update existing partner) ──────────────────────

      RoamingEndpoints.putCpoCredentials.zServerLogic { case (authHeader, body) =>
        (for
          _ <- validateOcpiToken(authHeader)
          _ <- ZIO.logInfo(s"OCPI partner credential update received")
        yield OcpiResponse.success(ourCpoCredentials(cfg)))
          .mapError(_.getMessage)
      },

      // ── EMSP CDR receive ───────────────────────────────────────────────────

      RoamingEndpoints.postEmspCdr.zServerLogic { case (authHeader, cdr) =>
        (for
          _ <- validateOcpiToken(authHeader)
          _ <- ZIO.logInfo(
            s"OCPI CDR received: ${cdr.id} " +
              s"energy=${cdr.total_energy}kWh cost=${cdr.total_cost} ${cdr.currency}"
          )
        // In production: persist CDR to database for billing/analytics
        yield ())
          .mapError(_.getMessage)
      },

      // ── EMSP CDR retrieve ─────────────────────────────────────────────────

      RoamingEndpoints.getEmspCdr.zServerLogic { case (id, authHeader) =>
        validateOcpiToken(authHeader)
          .flatMap(_ => ZIO.fail(new Exception(s"CDR $id not found")))
          .mapError(_.getMessage)
      },

      // ── Management: register endpoint ─────────────────────────────────────

      RoamingEndpoints.registerEndpoint.zServerLogic { case (tenantIdStr, body) =>
        val now      = Instant.now()
        val tenantId = TenantId(tenantIdStr)
        val ep = OcpiEndpoint(
          id = UUID.randomUUID().toString,
          tenantId = tenantIdStr,
          countryCode = body.countryCode,
          partyId = body.partyId,
          role = OcpiRole.fromCode(body.role).getOrElse(OcpiRole.EMSP),
          name = body.name,
          baseUrl = body.baseUrl,
          token = body.token,
          localToken = UUID.randomUUID().toString.replace("-", ""),
          status = OcpiEndpointStatus.Registered,
          backgroundPatchJob = false,
          locationsUrl = body.locationsUrl,
          sessionsUrl = body.sessionsUrl,
          cdrsUrl = body.cdrsUrl,
          tariffUrl = None,
          tokensUrl = body.tokensUrl,
          commandsUrl = None,
          lastPatchJobOn = None,
          lastPatchJobResult = None,
          createdOn = now,
          lastChangedOn = now
        )
        (ocpiRepo.upsert(tenantId, ep) *>
          ZIO.logInfo(s"OCPI endpoint registered: ${ep.name} for tenant $tenantIdStr") *>
          ZIO.succeed(ep))
          .mapError(_.getMessage)
      },

      // ── Management: list endpoints ────────────────────────────────────────

      RoamingEndpoints.listEndpoints.zServerLogic { tenantIdStr =>
        ocpiRepo.findAll(TenantId(tenantIdStr)).mapError(_.getMessage)
      }
    )
    ZioHttpInterpreter().toHttp(endpoints)

  // ── Helpers ───────────────────────────────────────────────────────────────

  private def validateOcpiToken(authHeader: String): Task[Unit] =
    if authHeader.startsWith("Token ") && authHeader.length > 7 then ZIO.unit
    else ZIO.fail(new Exception("Invalid OCPI Authorization header"))

  private def ourCpoCredentials(cfg: RoamingConfig): OcpiCredentials =
    OcpiCredentials(
      token = cfg.ocpiLocalToken,
      url = s"${cfg.externalUrl}/ocpi/cpo/2.1.1/versions",
      roles = List(OcpiCredentialRole(
        role = "CPO",
        party_id = cfg.ocpiPartyId,
        country_code = cfg.ocpiCountryCode,
        business_details = OcpiBusinessDetails(
          name = cfg.ocpiOperatorName,
          website = None,
          logo = None
        )
      ))
    )
