package io.itbeans.ev.restapi.pact

import io.circe.syntax._
import io.circe.generic.auto._
import io.github.jbwheatley.pact4s.circe.CirceJsonEncoding
import io.github.jbwheatley.pact4s.ziotest.PactForgerSuite
import io.github.jbwheatley.pact4s.{PactForger, RequestResponsePact}
import io.github.jbwheatley.pact4s.dsl.PactDsl
import io.github.jbwheatley.pact4s.model._
import zio._
import zio.test._

// ---------------------------------------------------------------------------
// Consumer contract: ev-rest-api → ev-auth-service
//
// The REST API calls Auth to validate bearer tokens on every authenticated
// request (via gRPC ValidateToken for internal calls and HTTP /auth/check-token
// for the public-facing path). This spec covers the HTTP path so that the
// Auth service can be verified independently by its provider test.
//
// Generated pact file: target/pacts/ev-rest-api-ev-auth-service.json
// ---------------------------------------------------------------------------

object AuthServicePactSpec extends PactForgerSuite with CirceJsonEncoding:

  // ── Shared test fixtures ──────────────────────────────────────────────────

  private val validToken =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJRCI6InRlc3QtdGVuYW50IiwicmVhbCI6dHJ1ZX0.sig"

  private val expiredToken =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJRCI6InRlc3QtdGVuYW50IiwiZXhwIjoxfQ.sig"

  // ── Pact definitions ──────────────────────────────────────────────────────

  override val pacts: List[RequestResponsePact] = List(
    // ── Interaction 1: valid token returns 200 + user info ──────────────────
    PactDsl
      .requestResponse
      .consumer("ev-rest-api")
      .hasPactWith("ev-auth-service")
      .uponReceiving("a valid bearer token to check")
      .method("POST")
      .path("/auth/check-token")
      .headers("Content-Type" -> "application/json")
      .body(Map("token" -> validToken).asJson)
      .willRespondWith()
      .status(200)
      .headers("Content-Type" -> "application/json")
      .body(
        """
          |{
          |  "id": "user-abc",
          |  "tenantID": "test-tenant",
          |  "email": "driver@example.com",
          |  "role": "Basic",
          |  "firstName": "Test",
          |  "lastName": "Driver"
          |}
        """.stripMargin
      )
      .toPact,

    // ── Interaction 2: expired / invalid token returns 401 ──────────────────
    PactDsl
      .requestResponse
      .consumer("ev-rest-api")
      .hasPactWith("ev-auth-service")
      .uponReceiving("an expired bearer token to check")
      .method("POST")
      .path("/auth/check-token")
      .headers("Content-Type" -> "application/json")
      .body(Map("token" -> expiredToken).asJson)
      .willRespondWith()
      .status(401)
      .toPact,

    // ── Interaction 3: signin returns JWT ───────────────────────────────────
    PactDsl
      .requestResponse
      .consumer("ev-rest-api")
      .hasPactWith("ev-auth-service")
      .uponReceiving("valid signin credentials")
      .method("POST")
      .path("/auth/signin")
      .headers("Content-Type" -> "application/json")
      .body(
        Map(
          "tenant"   -> "test-tenant",
          "email"    -> "driver@example.com",
          "password" -> "correct-horse"
        ).asJson
      )
      .willRespondWith()
      .status(200)
      .headers("Content-Type" -> "application/json")
      .bodyMatchingRegex("token", ".+")
      .toPact,

    // ── Interaction 4: bad credentials returns 401 ──────────────────────────
    PactDsl
      .requestResponse
      .consumer("ev-rest-api")
      .hasPactWith("ev-auth-service")
      .uponReceiving("invalid signin credentials")
      .method("POST")
      .path("/auth/signin")
      .headers("Content-Type" -> "application/json")
      .body(
        Map(
          "tenant"   -> "test-tenant",
          "email"    -> "driver@example.com",
          "password" -> "wrong-password"
        ).asJson
      )
      .willRespondWith()
      .status(401)
      .toPact
  )

  // ── ZIO Test suite — exercises the consumer against the pact mock server ──

  override val tests: Spec[PactForger, Throwable] =
    suite("AuthService HTTP contract (ev-rest-api consumer)")(
      test("POST /auth/check-token with valid token → 200 with user info") {
        for
          baseUrl <- ZIO.serviceWith[PactForger](_.mockServer.getUrl)
          resp <- ZIO.attempt(
            sttp.client3.quickRequest
              .post(uri"$baseUrl/auth/check-token")
              .header("Content-Type", "application/json")
              .body(Map("token" -> validToken).asJson.noSpaces)
              .send(sttp.client3.HttpURLConnectionBackend())
          )
        yield assertTrue(resp.code.code == 200)
      },
      test("POST /auth/check-token with expired token → 401") {
        for
          baseUrl <- ZIO.serviceWith[PactForger](_.mockServer.getUrl)
          resp <- ZIO.attempt(
            sttp.client3.quickRequest
              .post(uri"$baseUrl/auth/check-token")
              .header("Content-Type", "application/json")
              .body(Map("token" -> expiredToken).asJson.noSpaces)
              .send(sttp.client3.HttpURLConnectionBackend())
          )
        yield assertTrue(resp.code.code == 401)
      }
    )
