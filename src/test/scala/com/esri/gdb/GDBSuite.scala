package com.esri.gdb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers._

// @Ignore
class GDBSuite extends AnyFlatSpec with BeforeAndAfterAll {
  // private val folder = "src/test/resources"
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

  it should "Read SMP and write to parquet and count" in {
    val format = "parquet"
    sparkSession
      .sqlContext
      .gdb("/Users/mraad/data/SMP/SMP_R4_Q3.gdb", "Streets", numPartitions = 8)
      .select("Link_ID")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(f"/tmp/tmp.$format")
    val count = sparkSession.read.format("parquet").load(f"/tmp/tmp.$format").count()
    println(s"count=$count")
  }

  it should "Read SMP and count" in {
    val count = sparkSession
      .sqlContext
      .gdb("/Users/mraad/data/SMP/SMP_R4_Q3.gdb", "Streets")
      .count()
    println(s"count=$count")
  }

  it should "Read Miami.gdb" in {
    sparkSession
      .sqlContext
      .gdb("/Users/mraad/data/Miami.gdb", "Broadcast")
      .write
      .format("noop")
      .mode(SaveMode.Overwrite)
      .save()
  }

  it should "Read SMP_R4_Q3.gdb and write to noop" in {
    sparkSession
      .sqlContext
      .gdb(path = "/Users/mraad/data/SMP/SMP_R4_Q3.gdb", name = "Streets")
      .write
      .format("noop")
      .mode(SaveMode.Overwrite)
      .save()
  }

  it should "Read MultipointM" in {
    sparkSession
      .sqlContext
      .gdb(path = "/Users/mraad/GWorkspace/FileGDB/data/multipointM.gdb", name = "multipointM")
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
