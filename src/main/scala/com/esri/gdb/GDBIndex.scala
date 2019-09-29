package com.esri.gdb

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}

import com.esri.gdb.GDBTable.getClass
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.slf4j.LoggerFactory

private[gdb] trait SeekReader extends Serializable {
  def readSeek(byteBuffer: ByteBuffer): Long
}

private[gdb] class SeekReader4 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt
}

private[gdb] class SeekReader5 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt5
}

private[gdb] class SeekReader6 extends SeekReader {
  override def readSeek(byteBuffer: ByteBuffer): Long = byteBuffer.getUInt6
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
  private var numRow = 0
  private var seek = 0L

  def hasNext(): Boolean = {
    seek = 0L
    while (seek == 0L && numRow < maxRows /*&& dataInput.available > 0*/ ) {
      byteBuffer.clear()
      dataInput.readFully(bytes, 0, numBytesPerRow)
      seek = seekReader.readSeek(byteBuffer) // 0 value indicates that the row is deleted.
      numRow += 1 // Includes deleted rows
    }
    objectID += 1
    seek > 0L
  }

  def next(): GDBIndexRow = {
    GDBIndexRow(objectID, seek)
  }
}

/**
 * Class to cache GDB index rows.
 * This "hopefully" avoids multiple disk seeks while reading GDB data rows.
 *
 * TODO - Perform testing, does this matter with SSD ?
 *
 * @param dataInput      Hadoop FS input stream.
 * @param startID        the starting row ID
 * @param maxRows        the number of rows to read.
 * @param numBytesPerRow the number of bytes per index row.
 */
private[gdb] class GDBCacheIterator(dataInput: FSDataInputStream,
                                    startID: Int,
                                    maxRows: Int,
                                    numBytesPerRow: Int) extends Iterator[GDBIndexRow] with Serializable {
  private var rowIdx = 0
  private val rowArr = new Array[GDBIndexRow](maxRows) // TODO - Place off-heap

  private val iter = new GDBIndexIterator(dataInput, startID, maxRows, numBytesPerRow)
  while (iter.hasNext) {
    rowArr(rowIdx) = iter.next
    rowIdx += 1
  }
  rowIdx = 0

  override def hasNext: Boolean = rowIdx < maxRows

  override def next(): GDBIndexRow = {
    val r = rowArr(rowIdx)
    rowIdx += 1
    r
  }
}


class GDBIndex(dataInput: FSDataInputStream, maxRows: Int, numBytesPerRow: Int) extends AutoCloseable with Serializable {

  def indicies(numRowsToRead: Int = -1, startAtRow: Int = 0): Iterator[GDBIndexRow] = {
    val startRow = startAtRow min maxRows
    dataInput.seek(16L + startRow * numBytesPerRow)
    var numRows = if (numRowsToRead == -1)
      maxRows
    else
      numRowsToRead
    if (startRow + numRows > maxRows) {
      numRows = maxRows - startRow
    }
    // new GDBCacheIterator(dataInput, startRow, numRows, numBytesPerRow)
    new GDBIndexIterator(dataInput, startRow, numRows, numBytesPerRow)
  }

  override def close(): Unit = {
    dataInput.close()
  }
}

object GDBIndex extends Serializable {

  def apply(conf: Configuration, path: String, name: String): GDBIndex = {
    val logger = LoggerFactory.getLogger(getClass)
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)
    val bytes = new Array[Byte](16)
    dataInput.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    byteBuffer.getInt // signature
    val n1024Blocks = byteBuffer.getInt // n1024Blocks
    val maxRows = byteBuffer.getInt
    val numBytesPerRow = byteBuffer.getInt
    logger.debug(s"$name::n1024Blocks=$n1024Blocks, maxRows = $maxRows, numBytesPerRow=$numBytesPerRow")
    new GDBIndex(dataInput, maxRows, numBytesPerRow)
  }

}
