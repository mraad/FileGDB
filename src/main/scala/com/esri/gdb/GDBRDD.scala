package com.esri.gdb

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

case class GDBRDD(hadoopConfSer: SerializableConfiguration,
                  gdbPath: String,
                  gdbName: String,
                  numPartitions: Int
                 ) extends RDD[Row](SparkContext.getOrCreate(), Seq.empty) {

  override def compute(partition: Partition, context: TaskContext): Iterator[Row] = {
    partition match {
      case part: GDBPartition =>
        val someContext = Some(context)
        val index = GDBIndex(hadoopConfSer.value, gdbPath, part.hexName, someContext)
        val table = GDBTable(hadoopConfSer.value, gdbPath, part.hexName, someContext)
        context.addTaskCompletionListener(index)
        context.addTaskCompletionListener(table)
        table.rows(index, part.numRowsToRead, part.startAtRow)
      case _ => Iterator.empty
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val partitions = new ArrayBuffer[Partition](numPartitions)
    FileGDB.findTable(gdbPath, gdbName, hadoopConfSer.value) match {
      case Some(catTab) =>
        //        val table = GDBTable(conf, gdbPath, catTab.toTableName)
        //        try {
        val index = GDBIndex(hadoopConfSer.value, gdbPath, catTab.toTableName)
        try {
          //            if (index.maxRows != table.maxRows) {
          //              log.warn(s"Compress and then uncompress $gdbName for better read performance. Or better, copy it to a new feature class with the required fields.")
          //            }
          val maxRows = index.maxRows
          // println(s"${Console.YELLOW}getPartitions::tabRows=${table.maxRows} indRows=${index.maxRows}${Console.RESET}")
          if (maxRows > 0) {
            val maxRowsPerPartition = if (maxRows <= 1024)
              maxRows
            else
              (maxRows / numPartitions.toDouble).ceil.toInt
            var startAtRow = 0
            while (startAtRow < maxRows) {
              val numRowsToRead = (maxRows - startAtRow) min maxRowsPerPartition
              partitions append GDBPartition(partitions.length, catTab.toTableName, startAtRow, numRowsToRead)
              startAtRow += numRowsToRead
            }
          }
        } finally {
          index.close()
        }
      //        } finally {
      //          table.close()
      //        }
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
