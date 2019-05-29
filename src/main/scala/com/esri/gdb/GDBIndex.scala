package com.esri.gdb

import java.io.File
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

private[gdb] class GDBIndexIterator(dataInput: FSDataInputStream,
                                    startID: Int,
                                    maxRows: Int,
                                    numBytes: Int
                                   ) extends Iterator[GDBIndexRow] with Serializable {

  private val bytes = new Array[Byte](numBytes)
  private val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  private var objectID = startID
  private var numRow = 0
  private var seek = 0L

  def hasNext() = {
    seek = 0L
    while (seek == 0L && numRow < maxRows /*&& dataInput.available > 0*/ ) {
      byteBuffer.clear()
      dataInput.readFully(bytes, 0, numBytes)
      seek = byteBuffer.getUInt // 2019-05-29
      objectID += 1
      if (seek > 0L)
        numRow += 1
    }
    seek > 0L
  }

  def next() = {
    GDBIndexRow(objectID, seek)
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
    new GDBIndexIterator(dataInput, startRow, numRows, numBytesPerRow)
  }

  override def close(): Unit = {
    dataInput.close()
  }
}

object GDBIndex extends Serializable {

  def apply(conf: Configuration, path: String, name: String): GDBIndex = {

    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtablx").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)
    val bytes = new Array[Byte](16)
    dataInput.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

    byteBuffer.getInt // signature
    byteBuffer.getInt // n1024Blocks
    val maxRows = byteBuffer.getInt
    // println(s"${Console.YELLOW}maxRows = $maxRows${Console.RESET}")
    val numBytesPerRow = byteBuffer.getInt
    new GDBIndex(dataInput, maxRows, numBytesPerRow)
  }

}
