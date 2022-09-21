package com.esri.gdb

import org.apache.hadoop.fs.FSDataInputStream
// import org.sparkproject.guava.primitives.{Ints, Longs}

import java.nio.{ByteBuffer, ByteOrder}

class DataBuffer(dataInput: FSDataInputStream) extends AutoCloseable with Serializable {

  private var bytes = new Array[Byte](4096)
  private var byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def readBytes(length: Int): ByteBuffer = {
    //    if (length > bytes.length) {
    //      bytes = new Array[Byte](length)
    //      byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    //    }
    resize(length)
    byteBuffer.clear
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

  def position(seek: Long): DataBuffer = {
    dataInput.seek(seek)
    this
  }

  @inline
  private def fromBytes(b1: Byte, b2: Byte, b3: Byte, b4: Byte): Int = b1 << 24 | (b2 & 255) << 16 | (b3 & 255) << 8 | b4 & 255

  def getInt(): Int = {
    val b1 = dataInput.readByte()
    val b2 = dataInput.readByte()
    val b3 = dataInput.readByte()
    val b4 = dataInput.readByte()
    /*Ints.*/ fromBytes(b4, b3, b2, b1)
  }

  @inline
  private def fromBytes(b1: Byte, b2: Byte, b3: Byte, b4: Byte, b5: Byte, b6: Byte, b7: Byte, b8: Byte): Long =
    (b1 & 0xFFL) << 56 | (b2 & 0xFFL) << 48 | (b3 & 0xFFL) << 40 | (b4 & 0xFFL) << 32 | (b5 & 0xFFL) << 24 | (b6 & 0xFFL) << 16 | (b7 & 0xFFL) << 8 | (b8 & 0xFFL)

  def getLong(): Long = {
    val b1 = dataInput.readByte()
    val b2 = dataInput.readByte()
    val b3 = dataInput.readByte()
    val b4 = dataInput.readByte()
    val b5 = dataInput.readByte()
    val b6 = dataInput.readByte()
    val b7 = dataInput.readByte()
    val b8 = dataInput.readByte()
    /*Longs.*/ fromBytes(b8, b7, b6, b5, b4, b3, b2, b1)
  }

  def close() {
    dataInput.close()
  }
}

object DataBuffer extends Serializable {
  def apply(dataInput: FSDataInputStream): DataBuffer = {
    new DataBuffer(dataInput)
  }
}
