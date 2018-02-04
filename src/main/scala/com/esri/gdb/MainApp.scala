package com.esri.gdb

import org.apache.spark.sql.SparkSession

object MainApp extends App {

  val path = "/Users/mraad_admin/Share/World.gdb"
  val name = "Countries"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.ui.enabled", false)
    .getOrCreate()
  try {
    spark
      .read
      .gdb(path, name)
      .createTempView(name)

    spark
      .sql("select CNTRY_NAME,SQKM from Countries where SQKM < 10000.0 ORDER BY SQKM DESC LIMIT 10")
      .collect()
      .foreach(println)
  } finally {
    spark.stop()
  }

}
