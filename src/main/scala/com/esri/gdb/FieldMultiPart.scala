package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

class FieldMultiPart(val field: StructField,
                     xOrig: Double,
                     yOrig: Double,
                     xyScale: Double
                    ) extends FieldBytes {

  override type T = Row

  override def readValue(byteBuffer: ByteBuffer, oid: Int): Row = {
    val blob = getByteBuffer(byteBuffer)
    val geomType = blob.getVarUInt
    val numPoints = blob.getVarUInt.toInt
    if (numPoints > 0) {
      val coords = new ArrayBuffer[Double](numPoints * 2)
      val numParts = blob.getVarUInt.toInt

      val xmin = blob.getVarUInt // xyScale + xOrig
      val ymin = blob.getVarUInt // xyScale + yOrig
      val xmax = blob.getVarUInt // xyScale + xmin
      val ymax = blob.getVarUInt // xyScale + ymin

      var dx = 0L
      var dy = 0L

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
            val x = dx / xyScale + xOrig
            val y = dy / xyScale + yOrig
            coords.append(x, y)
            n += 1
          }
          p += 1
        }
        Row(parts, coords.toArray)
      }
      else {
        var n = 0
        while (n < numPoints) {
          dx += blob.getVarInt
          dy += blob.getVarInt
          val x = dx / xyScale + xOrig
          val y = dy / xyScale + yOrig
          coords.append(x, y)
          n += 1
        }
        Row(Array(numPoints), coords.toArray)
      }
    } else {
      Row(Array.empty[Int], Array.empty[Double])
    }
  }
}

object FieldMultiPart extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double
           ): FieldMultiPart = {
    new FieldMultiPart(StructField(name,
      StructType(Seq(StructField("parts", ArrayType(IntegerType), true), StructField("coords", ArrayType(DoubleType), true))
      ), nullable, metadata), xOrig, yOrig, xyScale)
  }
}
