package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class FieldMultiPartZM(val field: StructField,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       zOrig: Double,
                       zScale: Double,
                       mOrig: Double,
                       mScale: Double
                      ) extends FieldBytes {

  override type T = Row

  override def readNull(): T = null.asInstanceOf[Row]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val geomType = blob.getVarUInt
    val hasCurveDesc = (geomType & 0x20000000L) != 0L
    val numPoints = blob.getVarUInt.toInt
    // println(f"geomType=$geomType%X numPoints=$numPoints")
    if (numPoints > 0) {
      val coords = new Array[Double](numPoints * 4)
      val numParts = blob.getVarUInt.toInt
      val curveDesc = if (hasCurveDesc) blob.getVarUInt else 0
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

      if (numParts > 1) {
        val parts = new Array[Int](numParts)
        var p = 0
        var n = 1
        var sum = 0
        while (n < numParts) { // Read numParts-1
          val numXY = blob.getVarUInt.toInt
          parts(p) = numXY
          sum += numXY
          n += 1
          p += 1
        }
        parts(p) = numPoints - sum
        p = 0
        while (p < numParts) {
          val numPointsInPart = parts(p)
          n = 0
          while (n < numPointsInPart) {
            dx += blob.getVarInt
            dy += blob.getVarInt
            coords(ix) = dx / xyScale + xOrig
            coords(iy) = dy / xyScale + yOrig
            ix += 4
            iy += 4
            n += 1
          }
          p += 1
        }
        n = 0
        while (n < numPoints) {
          val varInt = try blob.getVarInt catch {case e: Throwable => -2}
          if (varInt < 0L) {
            coords(im) = Double.NaN
          } else {
            dz += varInt
            coords(iz) = dz / zScale + zOrig
          }
          iz += 4
          n += 1
        }
        n = 0
        while (n < numPoints) {
          val varInt = try blob.getVarInt catch {case e: Throwable => -2}
          if (varInt < 0L) {
            coords(im) = Double.NaN
          } else {
            dm += varInt
            coords(im) = dm / mScale + mOrig
          }
          im += 4
          n += 1
        }
        Row(xmin, ymin, xmax, ymax, parts, coords)
      }
      else {
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
          val varInt = try blob.getVarInt catch {case e: Throwable => -2}
          if (varInt < 0L) {
            coords(im) = Double.NaN
          } else {
            dz += varInt
            coords(iz) = dz / zScale + zOrig
          }
          iz += 4
          n += 1
        }
        n = 0
        while (n < numPoints) {
          val varInt = try blob.getVarInt catch {case e: Throwable => -2}
          if (varInt < 0L) {
            coords(im) = Double.NaN
          } else {
            dm += varInt
            coords(im) = dm / mScale + mOrig
          }
          im += 4
          n += 1
        }
        Row(xmin, ymin, xmax, ymax, Array(numPoints), coords)
      }
    } else {
      Row(0.0, 0.0, 0.0, 0.0, Array.empty[Int], Array.empty[Double])
    }
  }
}

object FieldMultiPartZM extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            zOrig: Double,
            zScale: Double,
            mOrig: Double,
            mScale: Double
           ): FieldMultiPartZM = {
    new FieldMultiPartZM(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, true),
        StructField("ymin", DoubleType, true),
        StructField("xmax", DoubleType, true),
        StructField("ymax", DoubleType, true),
        StructField("parts", ArrayType(IntegerType), true),
        StructField("coords", ArrayType(DoubleType), true))
      ), nullable, metadata), xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale)
  }
}
