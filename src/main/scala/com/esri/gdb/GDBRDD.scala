package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

case class GDBRDD(@transient sc: SparkContext,
                  gdbPath: String,
                  gdbName: String,
                  numPartitions: Int,
                  wkid: Int
                 ) extends RDD[Row](sc, Seq.empty) {

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    partition match {
      case part: GDBPartition => {
        // println(s"getPartitions::startAtRow=${part.startAtRow} numRowsToRead=${part.numRowsToRead}")
        val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
        val index = GDBIndex(conf, gdbPath, part.hexName)
        val table = GDBTable(conf, gdbPath, part.hexName, wkid)
        context.addTaskCompletionListener(_ => {
          table.close()
          index.close()
        })
        table.rows(index, part.numRowsToRead, part.startAtRow)
      }
      case _ => Iterator.empty
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = new ArrayBuffer[Partition](numPartitions)
    val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
    FileGDB.findTable(conf, gdbPath, gdbName, wkid) match {
      case Some(catTab) => {
        val table = GDBTable(conf, gdbPath, catTab.toTableName, wkid)
        try {
          val maxRows = table.maxRows
          // println(s"max rows=$maxRows")
          if (maxRows > 0) {
            val maxRowsPerPartition = 1 + maxRows / numPartitions
            var startAtRow = 0
            var index = 0
            while (startAtRow < maxRows) {
              val numRowsToRead = (maxRows - startAtRow) min maxRowsPerPartition
              partitions append GDBPartition(index, catTab.toTableName, startAtRow, numRowsToRead)
              startAtRow += maxRowsPerPartition
              index += 1
            }
          }
        } finally {
          table.close()
        }
      }
      case _ => {
        log.error(s"Cannot find '$gdbName' in $gdbPath, creating an empty array of Partitions !")
      }
    }
    partitions.toArray
  }
}

private[this] case class GDBPartition(index: Int,
                                      val hexName: String,
                                      val startAtRow: Int,
                                      val numRowsToRead: Int
                                     ) extends Partition