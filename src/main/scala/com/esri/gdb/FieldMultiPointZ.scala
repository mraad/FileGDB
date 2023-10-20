package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.ByteBuffer

class FieldMultiPointZ(val field: StructField, origScale: OrigScale) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldMultiPointZ(field, origScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt()
    val numPoints = blob.getVarUInt().toInt
    if (numPoints > 0) {
      val coords = new Array[Double](numPoints * 3)
      val xmin = blob.getVarUInt / origScale.xyScale + origScale.xOrig
      val ymin = blob.getVarUInt / origScale.xyScale + origScale.yOrig
      val xmax = blob.getVarUInt / origScale.xyScale + xmin
      val ymax = blob.getVarUInt / origScale.xyScale + ymin

      var dx = 0L
      var dy = 0L
      var dz = 0L
      var ix = 0
      var iy = 1
      var iz = 2
      var n = 0
      while (n < numPoints) {
        dx += blob.getVarInt
        dy += blob.getVarInt
        coords(ix) = dx / origScale.xyScale + origScale.xOrig
        coords(iy) = dy / origScale.xyScale + origScale.yOrig
        ix += 3
        iy += 3
        n += 1
      }
      n = 0
      while (n < numPoints) {
        dz += blob.getVarInt
        coords(iz) = dz / origScale.zScale + origScale.zOrig
        iz += 3
        n += 1
      }
      Row(xmin, ymin, xmax, ymax, Array(numPoints), coords)
    } else {
      Row(0.0, 0.0, 0.0, 0.0, Array.empty[Int], Array.empty[Double])
    }
  }
}

object FieldMultiPointZ extends Serializable {
  def apply(name: String, nullable: Boolean, metadata: Metadata, origScale: OrigScale): FieldMultiPointZ = {
    new FieldMultiPointZ(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, nullable),
        StructField("ymin", DoubleType, nullable),
        StructField("xmax", DoubleType, nullable),
        StructField("ymax", DoubleType, nullable),
        StructField("parts", ArrayType(IntegerType), nullable),
        StructField("coords", ArrayType(DoubleType), nullable)
      )), nullable, metadata), origScale)
  }
}
