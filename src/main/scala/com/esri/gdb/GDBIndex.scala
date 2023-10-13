package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.TaskCompletionListener
import org.slf4j.LoggerFactory

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable

private[gdb] trait SeekReader extends Serializable {
  def readSeek(byteBuffer: ByteBuffer): Long
}

private[gdb] class SeekReader4 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt()
}

private[gdb] class SeekReader5 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt5()
}

private[gdb] class SeekReader6 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt6()
}


private[gdb] class GDBIndexIterator(dataInput: FSDataInputStream,
                                    startID: Int,
                                    maxRows: Int,
                                    numBytesPerRow: Int
                                   )
  extends Iterator[GDBIndexRow]
    with TaskCompletionListener
    with Logging
    with Serializable {

  private val bytes = new Array[Byte](numBytesPerRow)
  private val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
  private val seekReader = numBytesPerRow match {
    case 5 => new SeekReader5()
    case 6 => new SeekReader6()
    case _ => new SeekReader4()
  }
  private var objectID = startID
  private var numRows = 0
  private var seek = 0L

  def hasNext(): Boolean = {
    while (seek == 0L && numRows < maxRows) {
      numRows += 1
      objectID += 1
      byteBuffer.clear()
      dataInput.readFully(bytes, 0, numBytesPerRow)
      seek = seekReader.readSeek(byteBuffer) // 0 value indicates that the row is deleted.
    }
    seek > 0L
  }

  def next(): GDBIndexRow = {
    val row = GDBIndexRow(objectID, seek)
    seek = 0L
    row
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    // logger.debug(s"onTaskCompletion:${context.partitionId()}:rows=$rows")
  }
}

class GDBIndex(dataInput: FSDataInputStream,
               header: GDBIndexHeader,
               context: Option[TaskContext]
              ) extends TaskCompletionListener with AutoCloseable with Serializable {

  // private val logger = LoggerFactory.getLogger(getClass)

  def maxRows: Int = header.maxRows

  def indices(numRows: Int = header.numRows, startRow: Int = 0): Iterator[GDBIndexRow] = {
    // logger.debug(s"indices::numRows=$numRows numBytesPerRow=${header.numBytesPerRow} startRow=$startRow")
    dataInput.seek(16L + startRow * header.numBytesPerRow)
    val iterator = new GDBIndexIterator(dataInput, startRow, numRows, header.numBytesPerRow)
    if (context.isDefined) {
      context.get.addTaskCompletionListener(iterator)
    }
    iterator
  }

  override def close(): Unit = {
    dataInput.close()
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    dataInput.close()
  }
}

case class GDBIndexHeader(version: Int, numPages: Int, numRows: Int, numBytesPerRow: Int) {
  def maxRows: Int = numPages * 1024

  override def toString: String = s"GDBHeader:version=$version, numPages=$numPages, numRows=$numRows, numBytesPerRow=$numBytesPerRow, maxRows=$maxRows"
}

object GDBIndex extends Serializable {

  private val map = mutable.Map.empty[String, GDBIndexHeader]

  def apply(conf: Configuration, path: String, name: String, context: Option[TaskContext] = None): GDBIndex = {
    val logger = LoggerFactory.getLogger(getClass)
    val filename = StringBuilder.newBuilder.append(path).append("/").append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)

    def readHeader: GDBIndexHeader = {
      logger.debug(s"Cache $filename...")
      val bytes = new Array[Byte](16)
      dataInput.readFully(bytes)
      val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val version = byteBuffer.getInt // signature / version
      val numPages = byteBuffer.getInt // A page has 1024 rows.
      val numRows = byteBuffer.getInt
      val numBytesPerRow = byteBuffer.getInt
      GDBIndexHeader(version, numPages, numRows, numBytesPerRow)
    }

    val header = map.getOrElseUpdate(filename, readHeader)
    new GDBIndex(dataInput, header, context)
  }

  //  private def readBitmap(dataInputStream: FSDataInputStream): GDBBitmap = {
  //    val bytes = new Array[Byte](GDBHeader.HeaderLength)
  //    dataInputStream.readFully(bytes)
  //    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
  //    val size = byteBuffer.getInt()
  //    val numBits = byteBuffer.getInt()
  //    val numSets = byteBuffer.getInt()
  //    val lastBit = byteBuffer.getInt()
  //    GDBBitmap(size, numBits, numSets, lastBit)
  //  }

}
