package com.esri

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}
import org.apache.spark.util.SerializableConfiguration

import java.nio.ByteBuffer

package object gdb {

  implicit class SparkContextImplicits(val sc: SparkContext) extends AnyVal {
    def gdb(path: String,
            name: String = "GDB_SystemCatalog",
            numPartitions: Int = sc.getConf.getInt("spark.default.parallelism", 8)
           ): GDBRDD = {
      GDBRDD(new SerializableConfiguration(sc.hadoopConfiguration), path, name, numPartitions)
    }
  }

  implicit class SQLContextImplicits(val sqlContext: SQLContext) extends AnyVal {
    def gdb(path: String,
            name: String = "GDB_SystemCatalog",
            numPartitions: Int = sqlContext.sparkContext.getConf.getInt("spark.default.parallelism", 8)
           ): DataFrame = {
      val relation = GDBRelation(path, name, numPartitions)(sqlContext)
      sqlContext.baseRelationToDataFrame(relation)
    }
  }

  implicit class DataFrameReaderImplicits(val reader: DataFrameReader) extends AnyVal {
    def gdb(path: String,
            name: String = "GDB_SystemCatalog",
            numPartitions: Int = 8
           ): DataFrame = reader
      .format("gdb")
      .option(GDBOptions.PATH, path)
      .option(GDBOptions.NAME, name)
      .option(GDBOptions.NUM_PARTITIONS, numPartitions.toString)
      .load()
  }

  implicit class ByteBufferImplicits(val byteBuffer: ByteBuffer) extends AnyVal {

    implicit def getVarUInt(): Long = {
      var shift = 7
      var b: Long = byteBuffer.get
      var ret = b & 0x7FL
      var old = ret
      while ((b & 0x80L) != 0) {
        b = byteBuffer.get
        ret = ((b & 0x7FL) << shift) | old
        old = ret
        shift += 7
      }
      ret
    }

    implicit def getVarInt(): Long = {
      var shift = 7
      var b: Long = byteBuffer.get
      val isNeg = (b & 0x40L) != 0
      var ret = b & 0x3FL
      var old = ret
      while ((b & 0x80L) != 0) {
        b = byteBuffer.get
        ret = ((b & 0x7FL) << (shift - 1)) | old
        old = ret
        shift += 7
      }
      if (isNeg) -ret else ret
    }

    implicit def getUByte(): Int = {
      byteBuffer.get & 0x00FF
    }

    implicit def getUInt(): Long = {
      val b1 = byteBuffer.get & 0xFFL
      val b2 = byteBuffer.get & 0xFFL
      val b3 = byteBuffer.get & 0xFFL
      val b4 = byteBuffer.get & 0xFFL
      b4 << 24 | b3 << 16 | b2 << 8 | b1
    }

    implicit def getUInt5(): Long = {
      val b1 = byteBuffer.get & 0xFFL
      val b2 = byteBuffer.get & 0xFFL
      val b3 = byteBuffer.get & 0xFFL
      val b4 = byteBuffer.get & 0xFFL
      val b5 = byteBuffer.get & 0xFFL
      b5 << 32 | b4 << 24 | b3 << 16 | b2 << 8 | b1
    }

    implicit def getUInt6(): Long = {
      val b1 = byteBuffer.get & 0xFFL
      val b2 = byteBuffer.get & 0xFFL
      val b3 = byteBuffer.get & 0xFFL
      val b4 = byteBuffer.get & 0xFFL
      val b5 = byteBuffer.get & 0xFFL
      val b6 = byteBuffer.get & 0xFFL
      b6 << 40 | b5 << 32 | b4 << 24 | b3 << 16 | b2 << 8 | b1
    }
  }

}
