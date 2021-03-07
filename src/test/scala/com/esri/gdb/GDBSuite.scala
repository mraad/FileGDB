package com.esri.gdb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, _}
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class GDBSuite extends AnyFlatSpec with BeforeAndAfterAll {
  private val folder = "src/test/resources"
  private val path = "src/test/resources/test.gdb"
  private val numRec = 4
  private var sparkSession: SparkSession = _

  Logger.getLogger("com.esri.gdb").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master("local")
      .appName("GDBSuite")
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", "/tmp")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      sparkSession.stop()
    } finally {
      super.afterAll()
    }
  }

  it should "test DSL" in {
    val results = sparkSession
      .sqlContext
      .gdb(path, "test")
      .select("*")
      .collect()

    results.size shouldBe numRec
  }

}
