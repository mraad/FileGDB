package com.esri.gdb

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.slf4j.LoggerFactory

case class GDBRelation(gdbPath: String,
                       gdbName: String,
                       numPartition: Int,
                       wkid: Int
                      )(@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with DataSourceRegister {

  override val schema: StructType = inferSchema()

  private def inferSchema(): StructType = {
    val sc = sqlContext.sparkContext
    FileGDB.findTable(sc.hadoopConfiguration, gdbPath, gdbName, wkid) match {
      case Some(catTab) => {
        val table = GDBTable(sc.hadoopConfiguration, gdbPath, catTab.toTableName, wkid)
        try {
          table.schema
        } finally {
          table.close()
        }
      }
      case _ => {
        LoggerFactory
          .getLogger(GDBRelation.getClass)
          .error(s"Cannot find '$gdbName' in $gdbPath, creating an empty schema !")
        StructType(Seq.empty[StructField])
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    GDBRDD(sqlContext.sparkContext, gdbPath, gdbName, numPartition, wkid)
  }

  override def shortName(): String = "gdb"
}
