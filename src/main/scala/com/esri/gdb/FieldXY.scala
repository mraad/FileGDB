package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, Metadata, StructField, StructType}

class FieldXY(val field: StructField,
              xOrig: Double,
              yOrig: Double,
              xyScale: Double
             ) extends FieldBytes {

  override type T = Row

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)

    blob.getVarUInt // geomType

    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    Row(x, y)
  }
}

object FieldXY extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double
           ): FieldXY = {
    new FieldXY(StructField(name,
      StructType(Seq(StructField("x", DoubleType, true), StructField("y", DoubleType, true))
      ), nullable, metadata), xOrig, yOrig, xyScale)
  }
}
