package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.TaskCompletionListener
import org.slf4j.LoggerFactory

import java.nio.{Buffer, ByteBuffer, ByteOrder}
import scala.collection.concurrent.TrieMap

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

  // Read the index in blocks rather than one 4-6 byte row per readFully call. The block is sized
  // by a byte budget, not a row count, so a bogus numBytesPerRow cannot blow up the allocation.
  private val rowsPerBlock = ((GDBIndexIterator.BlockBytes / numBytesPerRow) min maxRows) max 1
  private val bytes = new Array[Byte](rowsPerBlock * numBytesPerRow)
  private val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
  private val seekReader = numBytesPerRow match {
    case 5 => new SeekReader5()
    case 6 => new SeekReader6()
    case _ => new SeekReader4()
  }
  private var objectID = startID
  private var numRows = 0
  private var rowInBlock = 0
  private var rowsInBlock = 0
  private var seek = 0L

  def hasNext(): Boolean = {
    while (seek == 0L && numRows < maxRows) {
      if (rowInBlock == rowsInBlock) {
        rowsInBlock = rowsPerBlock min (maxRows - numRows)
        rowInBlock = 0
        dataInput.readFully(bytes, 0, rowsInBlock * numBytesPerRow)
      }
      // Position per row: a SeekReader may consume fewer bytes than the on-disk row stride, so
      // letting the buffer position just run on would desynchronize it from the slot boundaries.
      // Widened to Buffer - ByteBuffer.position(int) is covariant from Java 9 and breaks on 8.
      (byteBuffer: Buffer).position(rowInBlock * numBytesPerRow)
      rowInBlock += 1
      numRows += 1
      objectID += 1
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

private[gdb] object GDBIndexIterator {
  val BlockBytes = 32768
}

class GDBIndex(dataInput: FSDataInputStream,
               header: GDBIndexHeader,
               context: Option[TaskContext]
              ) extends TaskCompletionListener with AutoCloseable with Serializable {

  // private val logger = LoggerFactory.getLogger(getClass)

  def maxRows: Int = header.maxRows

  /**
   * @param numRows the number of index slots to scan, -1 for every slot from startRow on.
   * @param startRow the slot to start at.
   *
   * Both are clamped to what the file actually holds - an over-requested page used to run off the
   * end of the .gdbtablx and throw EOFException instead of returning the rows that were there.
   */
  def indices(numRows: Int = -1, startRow: Int = 0): Iterator[GDBIndexRow] = {
    val start = startRow max 0 min header.maxRows
    val available = header.maxRows - start
    val rows = (if (numRows < 0) available else numRows) min available max 0
    // logger.debug(s"indices::rows=$rows numBytesPerRow=${header.numBytesPerRow} start=$start")
    dataInput.seek(16L + start.toLong * header.numBytesPerRow)
    val iterator = new GDBIndexIterator(dataInput, start, rows, header.numBytesPerRow)
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

  // Concurrent - executors run several tasks against the same JVM.
  private val map = TrieMap.empty[String, GDBIndexHeader]

  def apply(conf: Configuration, path: String, name: String, context: Option[TaskContext] = None): GDBIndex = {
    val logger = LoggerFactory.getLogger(getClass)
    val filename = s"$path/$name.gdbtablx"
    val hdfsPath = new Path(filename)
    val fileSystem = hdfsPath.getFileSystem(conf)
    val dataInput = fileSystem.open(hdfsPath)

    def readHeader: GDBIndexHeader = {
      logger.debug(s"Cache $filename...")
      val bytes = new Array[Byte](16)
      dataInput.readFully(bytes)
      val byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
      val version = byteBuffer.getInt // signature / version
      val numPages = byteBuffer.getInt // A page has 1024 rows.
      val numRows = byteBuffer.getInt
      val numBytesPerRow = byteBuffer.getInt
      // The spec only ever uses 4, 5 or 6. Anything else means this is not a .gdbtablx we can read,
      // and letting it through would size the block buffer off a garbage number.
      if (numBytesPerRow < 4 || numBytesPerRow > 8) {
        throw new RuntimeException(
          s"'$filename' reports $numBytesPerRow bytes per row, expected 4 to 8. Not a readable index.")
      }
      GDBIndexHeader(version, numPages, numRows, numBytesPerRow)
    }

    val header = cachedHeader(map, cacheKey(fileSystem, hdfsPath), readHeader)
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
