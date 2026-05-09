package io.itbeans.ev.domain

import zio.schema.{DeriveSchema, Schema}

/** RFID badge / identification token used to start/stop charging. */
case class Tag(
    id: String, // the RFID/badge identifier (UID)
    tenantId: TenantId,
    userId: Option[UserId],
    description: Option[String],
    active: Boolean,
    default: Boolean,
    visualID: Option[String],
    // OCPP 2.1: IdToken type (RFID, ISO14443, ISO15693, KeyCode, Local, MacAddress, eMAID, NEMA, NoAuthorization, eID)
    tokenType: TagTokenType,
    groupId: Option[String],
    createdOn: java.time.Instant,
    lastChangedOn: java.time.Instant
)

enum TagTokenType:
  // OCPP 1.6
  case RFID, Other
  // OCPP 2.0.1 / 2.1 IdTokenEnum
  case ISO14443, ISO15693, KeyCode, Local, MacAddress, eMAID, NEMA, NoAuthorization, eID

object Tag:
  given Schema[TagTokenType] = DeriveSchema.gen
  given Schema[Tag] = DeriveSchema.gen
