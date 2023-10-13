package com.esri.gdb

import org.apache.spark.sql.types.StructField

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

trait GDBField extends Serializable {
  type T

  def field: StructField

  def name: String = field.name

  def nullable: Boolean = field.nullable

  def readValue(byteBuffer: ByteBuffer, oid: Int): T

  def readNull(): T

  def copy(): GDBField
}

class FieldFloat32(val field: StructField) extends GDBField {
  override type T = Float

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Float = {
    byteBuffer.getFloat
  }

  def readNull(): Float = null.asInstanceOf[Float]

  override def copy(): GDBField = new FieldFloat32(field)
}

class FieldFloat64(val field: StructField) extends GDBField {
  override type T = Double

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Double = {
    byteBuffer.getDouble
  }

  def readNull(): Double = null.asInstanceOf[Double]

  override def copy(): GDBField = new FieldFloat64(field)
}

class FieldInt16(val field: StructField) extends GDBField {
  override type T = Short

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Short = {
    byteBuffer.getShort
  }

  def readNull(): Short = null.asInstanceOf[Short]

  def copy(): GDBField = new FieldInt16(field)
}

class FieldInt32(val field: StructField) extends GDBField {
  override type T = Int

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Int = {
    byteBuffer.getInt
  }

  def readNull(): Int = null.asInstanceOf[Int]

  def copy(): GDBField = new FieldInt32(field)
}

class FieldUUID(val field: StructField) extends GDBField {
  override type T = String

  private val b = new Array[Byte](16)

  def readNull(): String = null.asInstanceOf[String]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    var n = 0
    while (n < 16) {
      b(n) = byteBuffer.get
      n += 1
    }
    "{%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X}".format(
      b(3), b(2), b(1), b(0),
      b(5), b(4), b(7), b(6),
      b(8), b(9), b(10), b(11),
      b(12), b(13), b(14), b(15))
  }

  override def copy(): GDBField = new FieldUUID(field)
}

class FieldTimestamp(val field: StructField) extends GDBField {
  override type T = Timestamp

  def readNull(): Timestamp = null.asInstanceOf[Timestamp]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Timestamp = {
    val numDays = byteBuffer.getDouble
    // convert days since 12/30/1899 to 1/1/1970
    val unixDays = numDays - 25569
    val millis = (unixDays * 1000 * 60 * 60 * 24).ceil.toLong
    new Timestamp(millis) // TimeZone is GMT !
  }

  override def copy(): GDBField = new FieldTimestamp(field)
}

//class FieldMillis(val field: StructField) extends GDBField {
//  override type T = Long
//
//  def readNull(): Long = null.asInstanceOf[Long]
//
//  override def readValue(byteBuffer: ByteBuffer, oid: Int): Long = {
//    val numDays = byteBuffer.getDouble
//    val unixDays = numDays - 25569
//    (unixDays * 1000 * 60 * 60 * 24).ceil.toLong // TimeZone is GMT !
//  }
//}

class FieldOID(val field: StructField) extends GDBField {
  override type T = Int

  def readNull(): Int = null.asInstanceOf[Int]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Int = oid

  override def copy(): GDBField = new FieldOID(field)
}

abstract class FieldBytes extends GDBField {
  protected var m_bytes = new Array[Byte](1024)

  def fillVarBytes(byteBuffer: ByteBuffer): Int = {
    val numBytes = byteBuffer.getVarUInt().toInt
    if (numBytes > m_bytes.length) {
      m_bytes = new Array[Byte](numBytes)
    }
    var n = 0
    while (n < numBytes) {
      m_bytes(n) = byteBuffer.get
      n += 1
    }
    numBytes
  }

  def getByteBuffer(byteBuffer: ByteBuffer): ByteBuffer = {
    val numBytes = fillVarBytes(byteBuffer)
    ByteBuffer.wrap(m_bytes, 0, numBytes)
  }

}

class FieldBinary(val field: StructField) extends FieldBytes {
  override type T = ByteBuffer

  def readNull(): ByteBuffer = null.asInstanceOf[ByteBuffer]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): ByteBuffer = {
    getByteBuffer(byteBuffer)
  }

  override def copy(): GDBField = new FieldBinary(field)
}

class FieldString(val field: StructField) extends FieldBytes {
  override type T = String

  def readNull(): String = null.asInstanceOf[String]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    val numBytes = fillVarBytes(byteBuffer)
    new String(m_bytes, 0, numBytes, StandardCharsets.UTF_8)
  }

  override def copy(): GDBField = new FieldString(field)
}

class FieldGeomNoop(val field: StructField) extends FieldBytes {
  override type T = String

  def readNull(): String = null.asInstanceOf[String]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    throw new RuntimeException("Should not have a NOOP geometry !")
  }

  def copy(): GDBField = new FieldGeomNoop(field)
}
