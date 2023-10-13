package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.ByteBuffer

class FieldMultiPointZM(val field: StructField,
                        xOrig: Double,
                        yOrig: Double,
                        xyScale: Double,
                        zOrig: Double,
                        zScale: Double,
                        mOrig: Double,
                        mScale: Double,
                       ) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldMultiPointZM(field, xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt()
    val numPoints = blob.getVarUInt().toInt
    if (numPoints > 0) {
      val coords = new Array[Double](numPoints * 4)
      val xmin = blob.getVarUInt / xyScale + xOrig
      val ymin = blob.getVarUInt / xyScale + yOrig
      val xmax = blob.getVarUInt / xyScale + xmin
      val ymax = blob.getVarUInt / xyScale + ymin

      var dx = 0L
      var dy = 0L
      var dz = 0L
      var dm = 0L
      var ix = 0
      var iy = 1
      var iz = 2
      var im = 3
      var n = 0
      while (n < numPoints) {
        dx += blob.getVarInt
        dy += blob.getVarInt
        coords(ix) = dx / xyScale + xOrig
        coords(iy) = dy / xyScale + yOrig
        ix += 4
        iy += 4
        n += 1
      }
      n = 0
      while (n < numPoints) {
        dz += blob.getVarInt
        coords(iz) = dz / zScale + zOrig
        iz += 4
        n += 1
      }
      n = 0
      while (n < numPoints) {
        dm += blob.getVarInt
        coords(im) = dm / mScale + mOrig
        im += 4
        n += 1
      }
      Row(xmin, ymin, xmax, ymax, Array(numPoints), coords)
    } else {
      Row(0.0, 0.0, 0.0, 0.0, Array.empty[Int], Array.empty[Double])
    }
  }
}

object FieldMultiPointZM extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            zOrig: Double,
            zScale: Double,
            mOrig: Double,
            mScale: Double,
           ): FieldMultiPointZM = {
    new FieldMultiPointZM(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, nullable),
        StructField("ymin", DoubleType, nullable),
        StructField("xmax", DoubleType, nullable),
        StructField("ymax", DoubleType, nullable),
        StructField("parts", ArrayType(IntegerType), nullable),
        StructField("coords", ArrayType(DoubleType), nullable)
      )), nullable, metadata), xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale)
  }
}
