package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.ByteBuffer

class FieldMultiPointM(val field: StructField,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       mOrig: Double,
                       mScale: Double,
                      ) extends FieldBytes {

  override type T = Row

  override def copy(): GDBField = new FieldMultiPointM(field, xOrig, yOrig, xyScale, mOrig, mScale)

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val _ = blob.getVarUInt()
    val numPoints = blob.getVarUInt().toInt
    if (numPoints > 0) {
      val coords = new Array[Double](numPoints * 3)
      val xmin = blob.getVarUInt / xyScale + xOrig
      val ymin = blob.getVarUInt / xyScale + yOrig
      val xmax = blob.getVarUInt / xyScale + xmin
      val ymax = blob.getVarUInt / xyScale + ymin

      var dx = 0L
      var dy = 0L
      var dm = 0L
      var ix = 0
      var iy = 1
      var im = 2
      var n = 0
      while (n < numPoints) {
        dx += blob.getVarInt
        dy += blob.getVarInt
        coords(ix) = dx / xyScale + xOrig
        coords(iy) = dy / xyScale + yOrig
        ix += 3
        iy += 3
        n += 1
      }
      n = 0
      blob.mark()
      if (blob.get() == 0x42) {
        while (n < numPoints) {
          coords(im) = Double.NaN
          im += 3
          n += 1
        }
      } else {
        blob.reset()
        while (n < numPoints) {
          dm += blob.getVarInt
          coords(im) = dm / mScale + mOrig
          im += 3
          n += 1
        }
      }
      Row(xmin, ymin, xmax, ymax, Array(numPoints), coords)
    } else {
      Row(0.0, 0.0, 0.0, 0.0, Array.empty[Int], Array.empty[Double])
    }
  }
}

object FieldMultiPointM extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            mOrig: Double,
            mScale: Double,
           ): FieldMultiPointM = {
    new FieldMultiPointM(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, nullable),
        StructField("ymin", DoubleType, nullable),
        StructField("xmax", DoubleType, nullable),
        StructField("ymax", DoubleType, nullable),
        StructField("parts", ArrayType(IntegerType), nullable),
        StructField("coords", ArrayType(DoubleType), nullable)
      )), nullable, metadata), xOrig, yOrig, xyScale, mOrig, mScale)
  }
}
