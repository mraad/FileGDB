package com.esri.gdb

import org.apache.hadoop.fs.FSDataInputStream
// import org.sparkproject.guava.primitives.{Ints, Longs}

import java.nio.{ByteBuffer, ByteOrder}

class DataBuffer(dataInput: FSDataInputStream) extends AutoCloseable with Serializable {

  private var bytes = new Array[Byte](4096)
  private var byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def readBytes(length: Int): ByteBuffer = {
    resize(length)
    byteBuffer.clear()
    dataInput.readFully(bytes, 0, length)
    byteBuffer
  }

  def resize(length: Int): Unit = {
    if (length > bytes.length) {
      bytes = new Array[Byte](length)
      byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    }
  }

  def seek(position: Long): DataBuffer = {
    dataInput.seek(position)
    this
  }

  // The GDB format is little-endian, DataInput is big-endian - hence the byte reversal.
  // One readInt/readLong beats 4/8 readByte calls through the FSDataInputStream stack.
  def getInt(): Int = Integer.reverseBytes(dataInput.readInt())

  def getLong(): Long = java.lang.Long.reverseBytes(dataInput.readLong())

  def close(): Unit = {
    dataInput.close()
  }
}

object DataBuffer extends Serializable {
  def apply(dataInput: FSDataInputStream): DataBuffer = {
    new DataBuffer(dataInput)
  }
}
