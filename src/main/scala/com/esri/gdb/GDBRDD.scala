package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

case class GDBRDD(@transient sc: SparkContext,
                  gdbPath: String,
                  gdbName: String,
                  numPartitions: Int
                 ) extends RDD[Row](sc, Seq.empty) {

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    partition match {
      case part: GDBPartition => {
        // println(s"${Console.YELLOW}compute::startAtRow = ${part.startAtRow} numRowsToRead = ${part.numRowsToRead}${Console.RESET}")
        val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
        val index = GDBIndex(conf, gdbPath, part.hexName)
        val table = GDBTable(conf, gdbPath, part.hexName)
        // Uncomment below when compiling for Spark 2.4.X - leave it commented for 2.3.X
        context.addTaskCompletionListener[Unit](_ => {
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
    FileGDB.findTable(gdbPath, gdbName, conf) match {
      case Some(catTab) =>
        val table = GDBTable(conf, gdbPath, catTab.toTableName)
        try {
          val maxRows = table.maxRows
          // log.debug(s"max rows=$maxRows")
          if (maxRows > 0) {
            // TODO make "100" configurable.
            val maxRowsPerPartition = if (maxRows < 100 || maxRows <= numPartitions)
              maxRows
            else
              1 + maxRows / numPartitions
            // log.debug(s"max rows per partition=$maxRowsPerPartition")
            var startAtRow = 0
            var index = 0
            while (startAtRow < maxRows) {
              val numRowsToRead = (maxRows - startAtRow) min maxRowsPerPartition
              log.debug(s"${Console.CYAN}startAtRow = $startAtRow numRowsToRead = $numRowsToRead${Console.RESET}")
              partitions append GDBPartition(index, catTab.toTableName, startAtRow, numRowsToRead)
              startAtRow += maxRowsPerPartition
              index += 1
            }
          }
        } finally {
          table.close()
        }
      case _ =>
        log.error(s"Cannot find '$gdbName' in $gdbPath, creating an empty array of Partitions !")
    }
    partitions.toArray
  }
}

private[this] case class GDBPartition(index: Int,
                                      hexName: String,
                                      startAtRow: Int,
                                      numRowsToRead: Int
                                     ) extends Partition
