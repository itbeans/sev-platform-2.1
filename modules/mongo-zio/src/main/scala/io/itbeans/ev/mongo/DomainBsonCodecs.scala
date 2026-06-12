package io.itbeans.ev.mongo

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.itbeans.ev.domain._
// Named import wins over the zio._ wildcard, which also exports a `Tag` type
import io.itbeans.ev.domain.Tag
import org.bson.Document
import org.bson.json.{JsonMode, JsonWriterSettings}
import zio._

import java.time.Instant

// ---------------------------------------------------------------------------
// DomainBsonCodecs — Circe ↔ BSON round-trip for every domain type.
//
// Pattern:
//   encodeDoc[T]  : T → BSON Document  (maps domain "id" → MongoDB "_id")
//   decodeDoc[T]  : BSON Document → T  (maps MongoDB "_id" → domain "id")
//
// Enums with a `.code` field are stored by code; all other enums use case name.
// Instant is stored as epoch-millisecond Long so Circe can decode it directly.
// ---------------------------------------------------------------------------

object DomainBsonCodecs:

  private val relaxedJson =
    JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()

  // -------------------------------------------------------------------------
  // Instant
  // -------------------------------------------------------------------------
  given Encoder[Instant] = Encoder.encodeLong.contramap(_.toEpochMilli)
  given Decoder[Instant] = Decoder.decodeLong.map(Instant.ofEpochMilli)

  // -------------------------------------------------------------------------
  // Opaque String IDs
  // -------------------------------------------------------------------------
  given Encoder[TenantId] = Encoder.encodeString.contramap(_.value)
  given Decoder[TenantId] = Decoder.decodeString.map(TenantId(_))
  given Encoder[UserId] = Encoder.encodeString.contramap(_.value)
  given Decoder[UserId] = Decoder.decodeString.map(UserId(_))
  given Encoder[SiteId] = Encoder.encodeString.contramap(_.value)
  given Decoder[SiteId] = Decoder.decodeString.map(SiteId(_))
  given Encoder[SiteAreaId] = Encoder.encodeString.contramap(_.value)
  given Decoder[SiteAreaId] = Decoder.decodeString.map(SiteAreaId(_))
  given Encoder[CompanyId] = Encoder.encodeString.contramap(_.value)
  given Decoder[CompanyId] = Decoder.decodeString.map(CompanyId(_))
  given Encoder[ChargingStationId] = Encoder.encodeString.contramap(_.value)
  given Decoder[ChargingStationId] = Decoder.decodeString.map(ChargingStationId(_))
  given Encoder[AssetId] = Encoder.encodeString.contramap(_.value)
  given Decoder[AssetId] = Decoder.decodeString.map(AssetId(_))
  given Encoder[PricingId] = Encoder.encodeString.contramap(_.value)
  given Decoder[PricingId] = Decoder.decodeString.map(PricingId(_))
  given Encoder[InvoiceId] = Encoder.encodeString.contramap(_.value)
  given Decoder[InvoiceId] = Decoder.decodeString.map(InvoiceId(_))
  given Encoder[CarId] = Encoder.encodeString.contramap(_.value)
  given Decoder[CarId] = Decoder.decodeString.map(CarId(_))
  given Encoder[ChargingProfileId] = Encoder.encodeString.contramap(_.value)
  given Decoder[ChargingProfileId] = Decoder.decodeString.map(ChargingProfileId(_))
  given Encoder[NotificationId] = Encoder.encodeString.contramap(_.value)
  given Decoder[NotificationId] = Decoder.decodeString.map(NotificationId(_))
  given Encoder[RegistrationTokenId] = Encoder.encodeString.contramap(_.value)
  given Decoder[RegistrationTokenId] = Decoder.decodeString.map(RegistrationTokenId(_))
  given Encoder[ConnectionId] = Encoder.encodeString.contramap(_.value)
  given Decoder[ConnectionId] = Decoder.decodeString.map(ConnectionId(_))

  // -------------------------------------------------------------------------
  // Opaque Long ID
  // -------------------------------------------------------------------------
  given Encoder[TransactionId] = Encoder.encodeLong.contramap(_.value)
  given Decoder[TransactionId] = Decoder.decodeLong.map(TransactionId(_))

  // -------------------------------------------------------------------------
  // Simple enums (stored as case name via .toString / .valueOf)
  // -------------------------------------------------------------------------
  given Encoder[UserRole] = Encoder.encodeString.contramap(_.toString)
  given Decoder[UserRole] = Decoder.decodeString.map(UserRole.valueOf)
  given Encoder[UserStatus] = Encoder.encodeString.contramap(_.toString)
  given Decoder[UserStatus] = Decoder.decodeString.map(UserStatus.valueOf)
  given Encoder[TenantComponent] = Encoder.encodeString.contramap(_.toString)
  given Decoder[TenantComponent] = Decoder.decodeString.map(TenantComponent.valueOf)
  given Encoder[OcppVersion] = Encoder.encodeString.contramap(_.toString)
  given Decoder[OcppVersion] = Decoder.decodeString.map(OcppVersion.valueOf)
  given Encoder[OcppTransport] = Encoder.encodeString.contramap(_.toString)
  given Decoder[OcppTransport] = Decoder.decodeString.map(OcppTransport.valueOf)
  given Encoder[ConnectorStatus] = Encoder.encodeString.contramap(_.toString)
  given Decoder[ConnectorStatus] = Decoder.decodeString.map(ConnectorStatus.valueOf)
  given Encoder[ConnectorType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[ConnectorType] = Decoder.decodeString.map(ConnectorType.valueOf)
  given Encoder[CurrentType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[CurrentType] = Decoder.decodeString.map(CurrentType.valueOf)
  given Encoder[TransactionAction] = Encoder.encodeString.contramap(_.toString)
  given Decoder[TransactionAction] = Decoder.decodeString.map(TransactionAction.valueOf)
  given Encoder[TransactionStop] = Encoder.encodeString.contramap(_.toString)
  given Decoder[TransactionStop] = Decoder.decodeString.map(TransactionStop.valueOf)
  given Encoder[TransactionLimitType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[TransactionLimitType] = Decoder.decodeString.map(TransactionLimitType.valueOf)
  given Encoder[TagTokenType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[TagTokenType] = Decoder.decodeString.map(TagTokenType.valueOf)
  given Encoder[NotificationType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[NotificationType] = Decoder.decodeString.map(NotificationType.valueOf)
  given Encoder[NotificationSeverity] = Encoder.encodeString.contramap(_.toString)
  given Decoder[NotificationSeverity] = Decoder.decodeString.map(NotificationSeverity.valueOf)
  given Encoder[NotificationChannel] = Encoder.encodeString.contramap(_.toString)
  given Decoder[NotificationChannel] = Decoder.decodeString.map(NotificationChannel.valueOf)
  given Encoder[StatisticScope] = Encoder.encodeString.contramap(_.toString)
  given Decoder[StatisticScope] = Decoder.decodeString.map(StatisticScope.valueOf)
  given Encoder[StatisticDataType] = Encoder.encodeString.contramap(_.toString)
  given Decoder[StatisticDataType] = Decoder.decodeString.map(StatisticDataType.valueOf)
  given Encoder[StatisticUnit] = Encoder.encodeString.contramap(_.toString)
  given Decoder[StatisticUnit] = Decoder.decodeString.map(StatisticUnit.valueOf)
  given Encoder[ConnectionStatus] = Encoder.encodeString.contramap(_.toString)
  given Decoder[ConnectionStatus] = Decoder.decodeString.map(ConnectionStatus.valueOf)
  given Encoder[PricingDayOfWeek] = Encoder.encodeString.contramap(_.toString)
  given Decoder[PricingDayOfWeek] = Decoder.decodeString.map(PricingDayOfWeek.valueOf)

  // -------------------------------------------------------------------------
  // Code-based enums (parameterized, stored via .code)
  // -------------------------------------------------------------------------
  given Encoder[AssetType] = Encoder.encodeString.contramap(_.code)

  given Decoder[AssetType] = Decoder.decodeString.emap(s =>
    AssetType.values.find(_.code == s).toRight(s"Unknown AssetType: $s")
  )

  given Encoder[AssetConnectorType] = Encoder.encodeString.contramap(_.code)

  given Decoder[AssetConnectorType] = Decoder.decodeString.emap(s =>
    AssetConnectorType.values.find(_.code == s).toRight(s"Unknown AssetConnectorType: $s")
  )

  given Encoder[PricingEntity] = Encoder.encodeString.contramap(_.code)

  given Decoder[PricingEntity] = Decoder.decodeString.emap(s =>
    PricingEntity.values.find(_.code == s).toRight(s"Unknown PricingEntity: $s")
  )

  given Encoder[InvoiceStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[InvoiceStatus] = Decoder.decodeString.emap(s =>
    InvoiceStatus.values.find(_.code == s).toRight(s"Unknown InvoiceStatus: $s")
  )

  given Encoder[BillingAccountStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[BillingAccountStatus] = Decoder.decodeString.emap(s =>
    BillingAccountStatus.values.find(_.code == s).toRight(s"Unknown BillingAccountStatus: $s")
  )

  given Encoder[BillingTransferStatus] = Encoder.encodeString.contramap(_.code)

  given Decoder[BillingTransferStatus] = Decoder.decodeString.emap(s =>
    BillingTransferStatus.values.find(_.code == s).toRight(s"Unknown BillingTransferStatus: $s")
  )

  given Encoder[CarType] = Encoder.encodeString.contramap(_.code)

  given Decoder[CarType] = Decoder.decodeString.emap(s =>
    CarType.values.find(_.code == s).toRight(s"Unknown CarType: $s")
  )

  given Encoder[ConverterType] = Encoder.encodeString.contramap(_.code)

  given Decoder[ConverterType] = Decoder.decodeString.emap(s =>
    ConverterType.values.find(_.code == s).toRight(s"Unknown ConverterType: $s")
  )

  given Encoder[ExternalConnectorType] = Encoder.encodeString.contramap(_.code)

  given Decoder[ExternalConnectorType] = Decoder.decodeString.emap(s =>
    ExternalConnectorType.values.find(_.code == s).toRight(s"Unknown ExternalConnectorType: $s")
  )

  // -------------------------------------------------------------------------
  // Case class codecs — dependency order (nested types before containers)
  // -------------------------------------------------------------------------
  given Codec[Coordinates] = deriveCodec
  given Codec[Address] = deriveCodec
  given Codec[Tenant] = deriveCodec
  given Codec[User] = deriveCodec
  given Codec[Tag] = deriveCodec
  given Codec[Site] = deriveCodec
  given Codec[SiteArea] = deriveCodec
  given Codec[Company] = deriveCodec
  given Codec[UserSite] = deriveCodec
  given Codec[Connector] = deriveCodec
  given Codec[ChargingStation] = deriveCodec
  given Codec[Transaction] = deriveCodec
  given Codec[Consumption] = deriveCodec
  given Codec[AssetMeasurement] = deriveCodec
  given Codec[AssetCurrentConsumption] = deriveCodec
  given Codec[Asset] = deriveCodec
  given Codec[Connection] = deriveCodec
  given Codec[PricingDimension] = deriveCodec
  given Codec[PricingDimensions] = deriveCodec
  given Codec[PricingStaticRestriction] = deriveCodec
  given Codec[PricingRestriction] = deriveCodec
  given Codec[PricingDefinition] = deriveCodec
  given Codec[BillingDimension] = deriveCodec
  given Codec[BillingSessionData] = deriveCodec
  given Codec[Invoice] = deriveCodec
  given Codec[BillingAccount] = deriveCodec
  given Codec[BillingTransfer] = deriveCodec
  given Codec[BillingUser] = deriveCodec
  given Codec[CarConverter] = deriveCodec
  given Codec[CarConnectorData] = deriveCodec
  given Codec[Car] = deriveCodec
  given Codec[CarCatalog] = deriveCodec
  given Codec[ChargingSchedulePeriod] = deriveCodec
  given Codec[ChargingSchedule] = deriveCodec
  given Codec[ChargingProfileData] = deriveCodec
  given Codec[ChargingProfile] = deriveCodec
  given Codec[Notification] = deriveCodec
  given Codec[NotificationPreferences] = deriveCodec
  given Codec[RegistrationToken] = deriveCodec
  given Codec[OcppEndpointSettings] = deriveCodec
  given Codec[SmartChargingSettings] = deriveCodec
  given Codec[BillingSettings] = deriveCodec
  given Codec[AnalyticsSettings] = deriveCodec
  given Codec[TenantSettings] = deriveCodec

  // -------------------------------------------------------------------------
  // BSON document helpers
  // -------------------------------------------------------------------------

  // Encode T → BSON Document, mapping domain "id" → MongoDB "_id"
  def encodeDoc[T: Encoder](value: T): Document =
    val bsonDoc = Document.parse(value.asJson.deepDropNullValues.noSpaces)
    if bsonDoc.containsKey("id") then
      val id = bsonDoc.get("id")
      bsonDoc.remove("id")
      bsonDoc.append("_id", id)
    bsonDoc

  // Decode BSON Document → T, mapping MongoDB "_id" → domain "id"
  def decodeDoc[T: Decoder](doc: Document): Either[String, T] =
    io.circe.parser.parse(doc.toJson(relaxedJson)).left.map(_.message).flatMap { j =>
      val remapped = j.hcursor.downField("_id").focus match
        case Some(idVal) => j.mapObject(_.remove("_id").add("id", idVal))
        case None        => j
      remapped.as[T].left.map(_.message)
    }

  def decodeDocZIO[T: Decoder](doc: Document): Task[T] =
    ZIO.fromEither(decodeDoc[T](doc)).mapError(new RuntimeException(_))
