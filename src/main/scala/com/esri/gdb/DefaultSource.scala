package com.esri.gdb

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "gdb"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(GDBOptions.PATH, sys.error("Parameter 'path' must be defined."))
    val name = parameters.getOrElse(GDBOptions.NAME, "GDB_SystemCatalog")
    val numPartitionsDefault = SparkSession.getActiveSession match {
      case Some(session) => session.conf.get("spark.default.parallelism", "8")
      case _ => "8"
    }
    val numPartitions = parameters.getOrElse(GDBOptions.NUM_PARTITIONS, numPartitionsDefault).toInt
    GDBRelation(path, name, numPartitions)(sqlContext)
  }

}
