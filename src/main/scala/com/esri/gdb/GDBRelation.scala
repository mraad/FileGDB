package com.esri.gdb

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

case class GDBRelation(gdbPath: String,
                       gdbName: String,
                       numPartition: Int
                      )(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with DataSourceRegister {

  override val schema: StructType = inferSchema()

  private def inferSchema(): StructType = {
    val sc = sqlContext.sparkContext
    FileGDB.findTable(gdbPath, gdbName, sc.hadoopConfiguration) match {
      case Some(catTab) => {
        val table = GDBTable(sc.hadoopConfiguration, gdbPath, catTab.toTableName)
        try {
          table.schema
        } finally {
          table.close()
        }
      }
      case _ => {
        LoggerFactory
          .getLogger(getClass)
          .error(s"Cannot find '$gdbName' in $gdbPath, creating an empty schema !")
        StructType(Seq.empty[StructField])
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    GDBRDD(new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration), gdbPath, gdbName, numPartition)
  }

  override def shortName(): String = "gdb"
}
