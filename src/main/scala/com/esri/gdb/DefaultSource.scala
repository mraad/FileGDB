package com.esri.gdb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}

class DefaultSource extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse(GDBOptions.PATH, sys.error("Parameter 'path' must be defined."))
    val name = parameters.getOrElse(GDBOptions.NAME, sys.error("Parameter 'name' must be defined."))
    val numPartitions = parameters.getOrElse(GDBOptions.NUM_PARTITIONS, "8").toInt
    val wkid = parameters.getOrElse(GDBOptions.WKID, "4326").toInt
    GDBRelation(path, name, numPartitions, wkid)(sqlContext)
  }

}
