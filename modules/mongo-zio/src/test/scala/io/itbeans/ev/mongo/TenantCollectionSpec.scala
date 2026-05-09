package io.itbeans.ev.mongo

import com.dimafeng.testcontainers.MongoDBContainer
import io.itbeans.ev.domain.TenantId
import org.bson.Document
import org.mongodb.scala.model.{Filters, Sorts, Updates}
import org.mongodb.scala.{MongoClient, MongoDatabase}
import zio.*
import zio.test.*

// ---------------------------------------------------------------------------
// Integration tests for TenantCollection and GlobalCollection.
//
// A real MongoDB instance is started once per suite via TestContainers.
// Each test uses a distinct collection name so tests are fully independent
// and can run in any order.
//
// Run: sbt "mongoZio/test"
// ---------------------------------------------------------------------------

object TenantCollectionSpec extends ZIOSpec[MongoDatabase]:

  // Start MongoDB container once for the entire spec; share the database handle.
  override val bootstrap: ZLayer[Any, Any, MongoDatabase] =
    ZLayer.scoped {
      for
        container <- ZIO.acquireRelease(
          ZIO.attempt {
            val c = MongoDBContainer()
            c.start()
            c
          }
        )(c => ZIO.attempt(c.stop()).ignore)
        client <- ZIO.acquireRelease(
          ZIO.attempt(MongoClient(container.replicaSetUrl))
        )(c => ZIO.succeed(c.close()))
      yield client.getDatabase("ev_it_test")
    }.orDie

  // ── Helpers ───────────────────────────────────────────────────────────────

  /** Build a BSON Document from a string _id and optional string fields. */
  private def doc(id: String, fields: (String, String)*): Document =
    val d = new Document("_id", id)
    fields.foreach { case (k, v) => d.append(k, v) }
    d

  /** Obtain a TenantCollection[Document] for the given collection + tenant. */
  private def tenantCol(
      collName: String,
      tenant: String
  ): ZIO[MongoDatabase, Nothing, TenantCollection[Document]] =
    ZIO.serviceWith[MongoDatabase](db =>
      TenantCollection[Document](collName, TenantId(tenant), db)
    )

  // ── Spec ──────────────────────────────────────────────────────────────────

  override def spec = suite("TenantCollection integration (MongoDB)")(
    // ── Basic CRUD ──────────────────────────────────────────────────────────

    test("insertOne and findOne — basic round-trip") {
      for
        col <- tenantCol("it_basic", "tenantA")
        _   <- col.insertOne(doc("basic-1", "name" -> "Alice"))
        got <- col.findOne(Filters.eq("_id", "basic-1"))
      yield assertTrue(got.isDefined) &&
        assertTrue(got.get.getString("name") == "Alice")
    },
    test("findOne returns None for a document that was never inserted") {
      for
        col <- tenantCol("it_missing", "tenantA")
        got <- col.findOne(Filters.eq("_id", "definitely-not-here"))
      yield assertTrue(got.isEmpty)
    },
    test("insertMany then find streams all matching documents") {
      for
        col <- tenantCol("it_find", "tenantA")
        _ <- col.insertMany(Seq(
          doc("find-1", "kind" -> "X"),
          doc("find-2", "kind" -> "X"),
          doc("find-3", "kind" -> "Y")
        ))
        xs <- col.find(Filters.eq("kind", "X")).runCollect
        ys <- col.find(Filters.eq("kind", "Y")).runCollect
      yield assertTrue(xs.size == 2) &&
        assertTrue(ys.size == 1)
    },
    test("find with sort and limit returns an ordered subset") {
      for
        col <- tenantCol("it_sort", "tenantA")
        _ <- col.insertMany(Seq(
          new Document("_id", "sort-a").append("score", Integer.valueOf(30)),
          new Document("_id", "sort-b").append("score", Integer.valueOf(10)),
          new Document("_id", "sort-c").append("score", Integer.valueOf(20))
        ))
        // top-2 descending by score
        got <- col.find(
          Filters.gte("score", Integer.valueOf(0)),
          Sorts.descending("score"),
          2
        ).runCollect
      yield assertTrue(got.size == 2) &&
        assertTrue(got(0).getInteger("score") == 30) &&
        assertTrue(got(1).getInteger("score") == 20)
    },
    test("replaceOne overwrites an existing document") {
      for
        col <- tenantCol("it_replace", "tenantA")
        _   <- col.insertOne(doc("rep-1", "v" -> "old"))
        _   <- col.replaceOne(Filters.eq("_id", "rep-1"), doc("rep-1", "v" -> "new"))
        got <- col.findOne(Filters.eq("_id", "rep-1"))
      yield assertTrue(got.get.getString("v") == "new")
    },
    test("replaceOne with upsert=true creates the document when absent") {
      for
        col <- tenantCol("it_upsert", "tenantA")
        _ <- col.replaceOne(
          Filters.eq("_id", "upsert-1"),
          doc("upsert-1", "v" -> "created"),
          upsert = true
        )
        got <- col.findOne(Filters.eq("_id", "upsert-1"))
      yield assertTrue(got.isDefined) &&
        assertTrue(got.get.getString("v") == "created")
    },
    test("updateOne modifies a single field without touching others") {
      for
        col   <- tenantCol("it_update", "tenantA")
        _     <- col.insertOne(doc("upd-1", "status" -> "pending", "extra" -> "keep"))
        _     <- col.updateOne(Filters.eq("_id", "upd-1"), Updates.set("status", "active"))
        after <- col.findOne(Filters.eq("_id", "upd-1"))
      yield assertTrue(after.get.getString("status") == "active") &&
        assertTrue(after.get.getString("extra") == "keep")
    },
    test("updateMany applies the update to every matching document") {
      for
        col <- tenantCol("it_updateMany", "tenantA")
        _ <- col.insertMany(Seq(
          doc("um-1", "grp" -> "g1", "active" -> "false"),
          doc("um-2", "grp" -> "g1", "active" -> "false"),
          doc("um-3", "grp" -> "g2", "active" -> "false")
        ))
        _      <- col.updateMany(Filters.eq("grp", "g1"), Updates.set("active", "true"))
        active <- col.find(Filters.eq("active", "true")).runCollect
        still  <- col.find(Filters.eq("active", "false")).runCollect
      yield assertTrue(active.size == 2) &&
        assertTrue(still.size == 1)
    },
    test("deleteOne removes exactly one document") {
      for
        col   <- tenantCol("it_delete", "tenantA")
        _     <- col.insertMany(Seq(doc("del-1"), doc("del-2")))
        _     <- col.deleteOne(Filters.eq("_id", "del-1"))
        after <- col.find(Filters.exists("_id")).runCollect
      yield assertTrue(!after.map(_.getString("_id")).contains("del-1")) &&
        assertTrue(after.map(_.getString("_id")).contains("del-2"))
    },
    test("countDocuments reflects live collection state") {
      for
        col    <- tenantCol("it_count", "tenantA")
        before <- col.countDocuments(Filters.exists("_id"))
        _      <- col.insertMany(Seq(doc("cnt-1"), doc("cnt-2"), doc("cnt-3")))
        after  <- col.countDocuments(Filters.exists("_id"))
        _      <- col.deleteOne(Filters.eq("_id", "cnt-1"))
        final_ <- col.countDocuments(Filters.exists("_id"))
      yield assertTrue(after == before + 3) &&
        assertTrue(final_ == after - 1)
    },

    // ── Tenant isolation — the core multi-tenancy invariant ─────────────────

    test("tenant isolation: tenantA documents are invisible in tenantB") {
      for
        colA  <- tenantCol("it_isolation", "tenantA")
        colB  <- tenantCol("it_isolation", "tenantB")
        _     <- colA.insertOne(doc("iso-1", "secret" -> "for-a-only"))
        fromB <- colB.findOne(Filters.eq("_id", "iso-1"))
        cntB  <- colB.countDocuments(Filters.exists("_id"))
      yield assertTrue(fromB.isEmpty) &&
        assertTrue(cntB == 0L)
    },
    test("tenant isolation: inserts to tenantB do not appear in tenantA") {
      for
        colA <- tenantCol("it_iso2", "tenantA")
        colB <- tenantCol("it_iso2", "tenantB")
        _    <- colA.insertOne(doc("a-doc"))
        _    <- colB.insertOne(doc("b-doc"))
        cntA <- colA.countDocuments(Filters.exists("_id"))
        cntB <- colB.countDocuments(Filters.exists("_id"))
      yield assertTrue(cntA == 1L) &&
        assertTrue(cntB == 1L)
    },

    // ── GlobalCollection ────────────────────────────────────────────────────

    test("GlobalCollection uses the bare name with no tenant prefix") {
      ZIO.serviceWithZIO[MongoDatabase] { db =>
        val global = GlobalCollection[Document]("it_global", db)
        // A document inserted via GlobalCollection must NOT appear under
        // a tenant-prefixed TenantCollection for any tenant.
        val tenantA = TenantCollection[Document]("it_global", TenantId("tenantA"), db)
        for
          _     <- global.insertOne(doc("glob-1", "src" -> "global"))
          fromT <- tenantA.findOne(Filters.eq("_id", "glob-1"))
        yield assertTrue(fromT.isEmpty)
      }
    }
  ) @@ TestAspect.sequential // one test at a time — avoids races on shared Mongo
    @@ TestAspect.timeout(120.seconds)
