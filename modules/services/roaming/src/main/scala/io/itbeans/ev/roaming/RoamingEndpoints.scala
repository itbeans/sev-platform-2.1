package io.itbeans.ev.roaming

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// Tapir schema for Instant
private given Schema[Instant] = Schema.string[Instant]

// ---------------------------------------------------------------------------
// RoamingEndpoints — Tapir endpoint definitions for the roaming service.
//
// OCPI CPO server (accepts EMSP → CPO requests):
//   GET  /ocpi/cpo/2.1.1/credentials      — return our credentials
//   POST /ocpi/cpo/2.1.1/credentials      — register an EMSP partner
//   PUT  /ocpi/cpo/2.1.1/credentials      — update an EMSP partner
//
// OCPI EMSP server (accepts CPO → EMSP requests):
//   POST /ocpi/emsp/2.1.1/cdrs            — CPO pushes a CDR to us (EMSP role)
//   GET  /ocpi/emsp/2.1.1/cdrs/{id}       — retrieve a CDR
//
// OCPI generic response codes: 1000=success, 2001=invalid params, 3000=server error.
// ---------------------------------------------------------------------------

// ── Request/response models ───────────────────────────────────────────────

case class OcpiCredentials(
    token: String,
    url: String, // our versions URL
    roles: List[OcpiCredentialRole]
)

object OcpiCredentials:
  given Encoder[OcpiCredentials] = deriveEncoder
  given Decoder[OcpiCredentials] = deriveDecoder

case class OcpiCredentialRole(
    role: String,
    party_id: String,
    country_code: String,
    business_details: OcpiBusinessDetails
)

object OcpiCredentialRole:
  given Encoder[OcpiCredentialRole] = deriveEncoder
  given Decoder[OcpiCredentialRole] = deriveDecoder

case class OcpiBusinessDetails(name: String, website: Option[String], logo: Option[String])

object OcpiBusinessDetails:
  given Encoder[OcpiBusinessDetails] = deriveEncoder
  given Decoder[OcpiBusinessDetails] = deriveDecoder

case class RegisterEndpointBody(
    countryCode: String,
    partyId: String,
    role: String, // "CPO" | "EMSP"
    name: String,
    baseUrl: String,
    token: String,
    cdrsUrl: Option[String],
    locationsUrl: Option[String],
    sessionsUrl: Option[String],
    tokensUrl: Option[String]
)

object RegisterEndpointBody:
  given Encoder[RegisterEndpointBody] = deriveEncoder
  given Decoder[RegisterEndpointBody] = deriveDecoder

// ── Header auth extractor (OCPI Bearer token) ─────────────────────────────

private val ocpiTokenHeader = header[String]("Authorization")

// ── Base paths ────────────────────────────────────────────────────────────

private val cpoBase  = endpoint.in("ocpi" / "cpo" / "2.1.1").errorOut(stringBody)
private val emspBase = endpoint.in("ocpi" / "emsp" / "2.1.1").errorOut(stringBody)
private val mgmtBase = endpoint.in("ocpi" / "mgmt").errorOut(stringBody)

// ── Endpoints ─────────────────────────────────────────────────────────────

object RoamingEndpoints:

  // ── CPO credential exchange ───────────────────────────────────────────────

  /** Return our CPO credentials (token in Authorization header is their local token). */
  val getCpoCredentials =
    cpoBase.get
      .in("credentials")
      .in(ocpiTokenHeader)
      .out(jsonBody[OcpiResponse[OcpiCredentials]])

  /** Register a new EMSP partner (credential POST from an EMSP). */
  val postCpoCredentials =
    cpoBase.post
      .in("credentials")
      .in(ocpiTokenHeader)
      .in(jsonBody[OcpiCredentials])
      .out(jsonBody[OcpiResponse[OcpiCredentials]])

  /** Update an existing partner registration. */
  val putCpoCredentials =
    cpoBase.put
      .in("credentials")
      .in(ocpiTokenHeader)
      .in(jsonBody[OcpiCredentials])
      .out(jsonBody[OcpiResponse[OcpiCredentials]])

  // ── EMSP CDR receive ──────────────────────────────────────────────────────

  /** Receive a CDR pushed by a CPO (our EMSP role). */
  val postEmspCdr =
    emspBase.post
      .in("cdrs")
      .in(ocpiTokenHeader)
      .in(jsonBody[OcpiCdr])
      .out(emptyOutput)

  /** Retrieve a stored CDR by ID. */
  val getEmspCdr =
    emspBase.get
      .in("cdrs" / path[String]("id"))
      .in(ocpiTokenHeader)
      .out(jsonBody[OcpiResponse[OcpiCdr]])

  // ── Management endpoints (internal, not OCPI spec) ────────────────────────

  /** Register or update a roaming partner (called by admin tooling). */
  val registerEndpoint =
    mgmtBase.post
      .in("endpoints")
      .in(header[String]("X-Tenant-ID"))
      .in(jsonBody[RegisterEndpointBody])
      .out(jsonBody[OcpiEndpoint])

  /** List all registered OCPI endpoints for a tenant. */
  val listEndpoints =
    mgmtBase.get
      .in("endpoints")
      .in(header[String]("X-Tenant-ID"))
      .out(jsonBody[List[OcpiEndpoint]])
