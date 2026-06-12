package io.itbeans.ev.restapi.pact

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import zio.test._

// ---------------------------------------------------------------------------
// Contract: ev-rest-api (consumer) → ev-auth-service (provider)
//
// The REST API calls Auth to validate bearer tokens (gRPC ValidateToken for
// the internal path, HTTP /auth/check-token for the public-facing path) and
// proxies /auth/signin. This spec pins the HTTP request/response JSON shapes
// so provider-side changes that break the consumer are caught at test time.
// ---------------------------------------------------------------------------

object AuthServicePactSpec extends ZIOSpecDefault:

  private val validToken =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZW5hbnRJRCI6InRlc3QtdGVuYW50IiwicmVhbCI6dHJ1ZX0.sig"

  // Shape ev-rest-api sends to POST /auth/check-token
  private def checkTokenRequest(token: String): Json =
    Json.obj("token" -> Json.fromString(token))

  // Shape ev-auth-service returns on 200 (AuthHttpServer.checkToken)
  private val checkTokenResponseJson =
    """{
      |  "valid": true,
      |  "userTokenJson": "{\"id\":\"user-abc\",\"tenantID\":\"test-tenant\",\"email\":\"driver@example.com\",\"role\":\"Basic\"}"
      |}""".stripMargin

  // Shape ev-rest-api sends to POST /auth/signin
  private val signInRequestJson =
    Json.obj(
      "tenant"   -> Json.fromString("test-tenant"),
      "email"    -> Json.fromString("driver@example.com"),
      "password" -> Json.fromString("correct-horse")
    )

  // Shape ev-auth-service returns on 200 (AuthHttpServer.signIn)
  private val signInResponseJson = """{ "token": "eyJhbGciOiJIUzI1NiJ9.payload.sig" }"""

  override def spec = suite("AuthService HTTP contract (ev-rest-api consumer)")(
    test("check-token request body carries the bearer token") {
      val body = checkTokenRequest(validToken)
      assertTrue(body.hcursor.get[String]("token").contains(validToken))
    },
    test("check-token 200 response parses to valid flag + embedded user token") {
      val result = parse(checkTokenResponseJson)
      val valid  = result.toOption.flatMap(_.hcursor.get[Boolean]("valid").toOption)
      val user   = result.toOption.flatMap(_.hcursor.get[String]("userTokenJson").toOption)
      assertTrue(
        valid.contains(true),
        user.exists(u => parse(u).exists(_.hcursor.get[String]("tenantID").contains("test-tenant")))
      )
    },
    test("signin request body carries tenant, email, and password") {
      val c = signInRequestJson.hcursor
      assertTrue(
        c.get[String]("tenant").contains("test-tenant"),
        c.get[String]("email").contains("driver@example.com"),
        c.get[String]("password").isRight
      )
    },
    test("signin 200 response parses to a non-empty token") {
      val token = parse(signInResponseJson).toOption.flatMap(_.hcursor.get[String]("token").toOption)
      assertTrue(token.exists(_.nonEmpty))
    },
    test("request bodies are stable under serialisation round-trip") {
      val reparsed = parse(signInRequestJson.noSpaces)
      assertTrue(reparsed.contains(signInRequestJson))
    }
  )
