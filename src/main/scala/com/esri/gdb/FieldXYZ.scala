package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField, StructType}

import java.nio.ByteBuffer

class FieldXYZ(val field: StructField,
               xOrig: Double,
               yOrig: Double,
               xyScale: Double,
               zOrig: Double,
               zScale: Double
              ) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldXYZ(field, xOrig, yOrig, xyScale, zOrig, zScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt() // geomType
    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val vz = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig
    val z = (vz - 1.0) / zScale + zOrig
    Row(x, y, z)
  }
}

object FieldXYZ extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            zOrig: Double,
            zScale: Double,
           ): FieldXYZ = {
    new FieldXYZ(StructField(name,
      StructType(Seq(
        StructField("x", DoubleType, nullable),
        StructField("y", DoubleType, nullable),
        StructField("z", DoubleType, nullable),
      )), nullable, metadata), xOrig, yOrig, xyScale, zOrig, zScale)
  }
}
