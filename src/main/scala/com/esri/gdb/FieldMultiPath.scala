package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.{OperatorExportToJson, Polygon, Polyline, SpatialReference}
import org.apache.spark.sql.types.StructField

class FieldMultiPath(val field: StructField,
                     xOrig: Double,
                     yOrig: Double,
                     xyScale: Double,
                     wkid: Int
                    ) extends FieldBytes {

  val oper = OperatorExportToJson.local
  val spatialReference = SpatialReference.create(wkid)

  override type T = String

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    val blob = getByteBuffer(byteBuffer)
    val geomType = blob.getVarUInt
    val multiPath = geomType match {
      case 3 => new Polyline()
      case _ => new Polygon()
    }
    val numPoints = blob.getVarUInt.toInt
    if (numPoints > 0) {
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
        while (n < numParts) {
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
            n match {
              case 0 => multiPath.startPath(x, y)
              case _ => multiPath.lineTo(x, y)
            }
            n += 1
          }
          p += 1
        }
      }
      else {
        var n = 0
        while (n < numPoints) {
          dx += blob.getVarInt
          dy += blob.getVarInt
          val x = dx / xyScale + xOrig
          val y = dy / xyScale + yOrig
          n match {
            case 0 => multiPath.startPath(x, y)
            case _ => multiPath.lineTo(x, y)
          }
          n += 1
        }
      }
    }
    oper.execute(spatialReference, multiPath)
  }
}