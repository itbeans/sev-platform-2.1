package io.itbeans.ev.authservice

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.ztapir._
import sttp.tapir.Schema

import java.time.Instant

// Tapir Schema instances for custom types
private given Schema[Instant] = Schema.string[Instant]

// ---------------------------------------------------------------------------
// AuthEndpoints — Tapir endpoint definitions for /auth.
//
//   POST /auth/signin      — login (returns JWT)
//   POST /auth/signout     — logout (stateless; always 200)
//   GET  /auth/check-token — validate a Bearer token (for gRPC-less clients)
// ---------------------------------------------------------------------------

// ── Request/response models ───────────────────────────────────────────────

case class SignInBody(
    tenant: String, // tenant subdomain (empty string = super-admin)
    email: String,
    password: String
)

object SignInBody:
  given Encoder[SignInBody] = deriveEncoder
  given Decoder[SignInBody] = deriveDecoder

case class SignInResponse(token: String)

object SignInResponse:
  given Encoder[SignInResponse] = deriveEncoder
  given Decoder[SignInResponse] = deriveDecoder

case class CheckTokenBody(token: String)

object CheckTokenBody:
  given Encoder[CheckTokenBody] = deriveEncoder
  given Decoder[CheckTokenBody] = deriveDecoder

case class CheckTokenResponse(valid: Boolean, userTokenJson: Option[String])

object CheckTokenResponse:
  given Encoder[CheckTokenResponse] = deriveEncoder
  given Decoder[CheckTokenResponse] = deriveDecoder

// ── Base endpoint ─────────────────────────────────────────────────────────

private val base =
  endpoint
    .in("auth")
    .errorOut(stringBody)

// ── Endpoint definitions ──────────────────────────────────────────────────

object AuthEndpoints:

  val signIn =
    base.post
      .in("signin")
      .in(jsonBody[SignInBody])
      .out(jsonBody[SignInResponse])

  val signOut =
    base.post
      .in("signout")
      .out(emptyOutput)

  val checkToken =
    base.post
      .in("check-token")
      .in(jsonBody[CheckTokenBody])
      .out(jsonBody[CheckTokenResponse])
