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
  override type T = java.lang.Float

  override def readValue(byteBuffer: ByteBuffer, oid: Int): java.lang.Float = {
    byteBuffer.getFloat
  }

  def readNull(): java.lang.Float = null

  override def copy(): GDBField = new FieldFloat32(field)
}

class FieldFloat64(val field: StructField) extends GDBField {
  override type T = java.lang.Double

  override def readValue(byteBuffer: ByteBuffer, oid: Int): java.lang.Double = {
    byteBuffer.getDouble
  }

  def readNull(): java.lang.Double = null

  override def copy(): GDBField = new FieldFloat64(field)
}

class FieldInt16(val field: StructField) extends GDBField {
  override type T = java.lang.Short

  override def readValue(byteBuffer: ByteBuffer, oid: Int): java.lang.Short = {
    byteBuffer.getShort
  }

  def readNull(): java.lang.Short = null

  def copy(): GDBField = new FieldInt16(field)
}

class FieldInt32(val field: StructField) extends GDBField {
  override type T = java.lang.Integer

  override def readValue(byteBuffer: ByteBuffer, oid: Int): java.lang.Integer = {
    byteBuffer.getInt
  }

  def readNull(): java.lang.Integer = null

  def copy(): GDBField = new FieldInt32(field)
}

class FieldUUID(val field: StructField) extends GDBField {
  override type T = String

  private val b = new Array[Byte](16)
  private val sb = new java.lang.StringBuilder(38)

  def readNull(): String = null

  private def hex(i: Int): Unit = {
    val v = b(i) & 0xFF
    sb.append(FieldUUID.Hex(v >> 4)).append(FieldUUID.Hex(v & 0x0F))
  }

  // Hand-rolled - String.format re-parses its 16-arg pattern on every single row.
  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    byteBuffer.get(b, 0, 16)
    sb.setLength(0)
    sb.append('{')
    hex(3); hex(2); hex(1); hex(0); sb.append('-')
    hex(5); hex(4); sb.append('-')
    hex(7); hex(6); sb.append('-')
    hex(8); hex(9); sb.append('-')
    hex(10); hex(11); hex(12); hex(13); hex(14); hex(15)
    sb.append('}')
    sb.toString
  }

  override def copy(): GDBField = new FieldUUID(field)
}

object FieldUUID {
  private val Hex = "0123456789ABCDEF".toCharArray
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
  override type T = java.lang.Integer

  def readNull(): java.lang.Integer = null

  override def readValue(byteBuffer: ByteBuffer, oid: Int): java.lang.Integer = oid

  override def copy(): GDBField = new FieldOID(field)
}

abstract class FieldBytes extends GDBField {
  protected var m_bytes = new Array[Byte](1024)

  def fillVarBytes(byteBuffer: ByteBuffer): Int = {
    val numBytes = byteBuffer.getVarUInt().toInt
    if (numBytes > m_bytes.length) {
      m_bytes = new Array[Byte](numBytes)
    }
    byteBuffer.get(m_bytes, 0, numBytes) // Bulk copy, not a byte at a time.
    numBytes
  }

  def getByteBuffer(byteBuffer: ByteBuffer): ByteBuffer = {
    val numBytes = fillVarBytes(byteBuffer)
    ByteBuffer.wrap(m_bytes, 0, numBytes)
  }

}

class FieldBinary(val field: StructField) extends FieldBytes {
  // BinaryType maps to Array[Byte] in Spark, and the value has to outlive the shared m_bytes buffer.
  override type T = Array[Byte]

  def readNull(): Array[Byte] = null

  // Straight into the result: going via fillVarBytes would copy every blob twice and pin the
  // shared m_bytes at the size of the largest blob in the table for the life of the task.
  override def readValue(byteBuffer: ByteBuffer, oid: Int): Array[Byte] = {
    val bytes = new Array[Byte](byteBuffer.getVarUInt().toInt)
    byteBuffer.get(bytes)
    bytes
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
