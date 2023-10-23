package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField, StructType}

import java.nio.ByteBuffer

class FieldXYM(val field: StructField, origScale: OrigScale) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldXYM(field, origScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt() // Geometry type.
    val vx = blob.getVarUInt()
    if (vx != 0L) { // Check if not empty.
      val vy = blob.getVarUInt()
      val vm = blob.getVarUInt()
      val x = (vx - 1.0) / origScale.xyScale + origScale.xOrig
      val y = (vy - 1.0) / origScale.xyScale + origScale.yOrig
      val m = (vm - 1.0) / origScale.mScale + origScale.mOrig
      Row(x, y, m)
    } else {
      Row(Double.NaN, Double.NaN, Double.NaN)
    }
  }
}

object FieldXYM extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            origScale: OrigScale
           ): FieldXYM = {
    new FieldXYM(StructField(name,
      StructType(Seq(
        StructField("x", DoubleType, nullable),
        StructField("y", DoubleType, nullable),
        StructField("m", DoubleType, nullable)
      )), nullable, metadata), origScale)
  }
}
