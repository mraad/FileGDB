package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField, StructType}

import java.nio.ByteBuffer

class FieldXY(val field: StructField, origScale: OrigScale) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldXY(field, origScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt() // geomType
    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / origScale.xyScale + origScale.xOrig
    val y = (vy - 1.0) / origScale.xyScale + origScale.yOrig
    Row(x, y)
  }
}

object FieldXY extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            origScale: OrigScale
           ): FieldXY = {
    new FieldXY(StructField(name,
      StructType(Seq(
        StructField("x", DoubleType, nullable),
        StructField("y", DoubleType, nullable))
      ), nullable, metadata), origScale)
  }
}
