package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import java.nio.{ByteBuffer, ByteOrder}

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
                                   ) extends Iterator[GDBIndexRow] with Serializable {

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
  // println(s"GDBIndexIterator::startID=$startID maxRows=$maxRows")

  def hasNext(): Boolean = {
    while (numRows < maxRows) {
      numRows += 1
      objectID += 1
      byteBuffer.clear()
      dataInput.readFully(bytes, 0, numBytesPerRow)
      seek = seekReader.readSeek(byteBuffer) // 0 value indicates that the row is deleted.
      if (seek > 0) {
        return true
      }
    }
    false
  }

  def next(): GDBIndexRow = {
    GDBIndexRow(objectID, seek)
  }
}

class GDBIndex(dataInput: FSDataInputStream, val maxRows: Int, numBytesPerRow: Int) extends AutoCloseable with Serializable {

  def indices(numRows: Int = maxRows, startRow: Int = 0): Iterator[GDBIndexRow] = {
    // println(s"indices::startRow=$startRow numRows=$numRows numBytesPerRow=$numBytesPerRow")
    dataInput.seek(16L + startRow * numBytesPerRow)
    new GDBIndexIterator(dataInput, startRow, numRows, numBytesPerRow)
  }

  override def close(): Unit = {
    dataInput.close()
  }
}

object GDBIndex extends Serializable {

  def apply(conf: Configuration, path: String, name: String): GDBIndex = {
    // val logger = LoggerFactory.getLogger(getClass)
    val filename = StringBuilder.newBuilder.append(path).append("/").append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)
    val bytes = new Array[Byte](16)
    dataInput.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    byteBuffer.getInt // signature
    val n1024Blocks = byteBuffer.getInt // n1024Blocks
    val maxRows = byteBuffer.getInt
    val numBytesPerRow = byteBuffer.getInt
    // println(s"${Console.RED}$name::n1024Blocks=$n1024Blocks, maxRows=$maxRows, numBytesPerRow=$numBytesPerRow${Console.RESET}")
    new GDBIndex(dataInput, maxRows, numBytesPerRow)
  }

}
