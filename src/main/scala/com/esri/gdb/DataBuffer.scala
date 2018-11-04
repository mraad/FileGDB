package com.esri.gdb

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.hadoop.fs.FSDataInputStream

class DataBuffer(dataInput: FSDataInputStream) extends AutoCloseable with Serializable {

  private var bytes = new Array[Byte](4096)
  private var byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def readBytes(length: Int): ByteBuffer = {
    if (length > bytes.length) {
      bytes = new Array[Byte](length)
      byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    }
    else {
      byteBuffer.clear
    }
    dataInput.readFully(bytes, 0, length)
    byteBuffer
  }

  def seek(position: Long): DataBuffer = {
    dataInput.seek(position)
    this
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
