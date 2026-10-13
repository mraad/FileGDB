package com.esri.gdb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{col, count, hash, lit, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

// @Ignore
class GDBSuite extends AnyFlatSpec with BeforeAndAfterAll {
  private val path = "src/test/resources/test.gdb"
  private var sparkSession: SparkSession = _

  Logger.getLogger("com.esri.gdb").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("GDBSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GDBRegistrator].getName)
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  it should "test Random in gdb in resources" in {
    sparkSession
      .sqlContext
      .gdb(path, "Random")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("/tmp/tmp.prq")
    val count = sparkSession.read.parquet("/tmp/tmp.prq").count()
    count shouldBe 4710
  }

//  it should "Read SMP and write to parquet and count" in {
//    val format = "parquet"
//    sparkSession
//      .sqlContext
//      .gdb("/Users/mraad/data/SMP/SMP_R4_Q3.gdb", "Streets", numPartitions = 8)
//      .select("Link_ID")
//      .write
//      .format("parquet")
//      .mode(SaveMode.Overwrite)
//      .save(f"/tmp/tmp.$format")
//    val count = sparkSession.read.format("parquet").load(f"/tmp/tmp.$format").count()
//    println(s"count=$count")
//  }

  //  it should "Read SMP and count" in {
  //    val count = sparkSession
  //      .sqlContext
  //      .gdb("/Users/mraad/data/SMP/SMP_R4_Q3.gdb", "Streets")
  //      .count()
  //    println(s"count=$count")
  //  }

  it should "Read Miami.gdb" in {
    sparkSession
      .sqlContext
      .gdb("data/Miami.gdb", "Broadcast")
      .write
      .format("noop")
      .mode(SaveMode.Overwrite)
      .save()
  }

  it should "read the same rows from Miami.gdb Broadcast with sortByOffset" in {
    // Broadcast is an edited table whose rows sit in near-random file order, so sorting by offset
    // really reorders every partition - a row count plus an order-free hash of every column catches
    // a dropped, duplicated or misdecoded row.
    def digest(sortByOffset: Boolean): (Long, Long) = {
      val df: DataFrame = sparkSession.read.gdb("data/Miami.gdb", "Broadcast", sortByOffset = sortByOffset)
      val row = df.select(count(lit(1)), sum(hash(df.columns.map(c => col(s"`$c`")): _*).cast("long"))).first()
      (row.getLong(0), row.getLong(1))
    }

    val sorted = digest(sortByOffset = true)
    sorted._1 shouldBe 1365578L
    sorted shouldBe digest(sortByOffset = false)
  }

//  it should "Read SMP_R4_Q3.gdb and write to noop" in {
//    sparkSession
//      .sqlContext
//      .gdb(path = "/Users/mraad/data/SMP/SMP_R4_Q3.gdb", name = "Streets")
//      .write
//      .format("noop")
//      .mode(SaveMode.Overwrite)
//      .save()
//  }

  it should "Read FileGDBTest/PolygonZ" in {
    sparkSession
      .sqlContext
      .gdb(path = "data/FileGDBTest.gdb", name = "PolygonZ")
      .write
      .format("noop")
      .mode(SaveMode.Overwrite)
      .save()
  }

  it should "Read PointM/Points" in {
    sparkSession
      .sqlContext
      .gdb(path = "notebooks/pointm.gdb", name = "points")
      .write
      .format("noop")
      .mode(SaveMode.Overwrite)
      .save()
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.stop()
    } finally {
      super.afterAll()
    }
  }

}
