package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequence, GeometryFactory, PrecisionModel}

class FieldMultiPart(val field: StructField,
                     xOrig: Double,
                     yOrig: Double,
                     xyScale: Double,
                     xyTolerance: Double
                    ) extends FieldBytes {

  override type T = String

  protected var dx = 0L
  
  protected var dy = 0L

  @transient val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))

  @transient val emptyPolygon = geomFact.createPolygon(null.asInstanceOf[CoordinateSequence]).toString()

  override def readNull(): T = null.asInstanceOf[String]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    val blob = getByteBuffer(byteBuffer)
    val geomType = blob.getVarUInt
    val hasCurveDesc = (geomType & 0x20000000L) != 0L
    val numPoints = blob.getVarUInt.toInt
    // println(f"geomType=$geomType%X numPoints=$numPoints")
    if (numPoints > 0) {
      val coords = new Array[Double](numPoints * 2)
      val numParts = blob.getVarUInt.toInt
      val curveDesc = if (hasCurveDesc) blob.getVarUInt else 0
      val xmin = blob.getVarUInt / xyScale + xOrig
      val ymin = blob.getVarUInt / xyScale + yOrig
      val xmax = blob.getVarUInt / xyScale + xmin
      val ymax = blob.getVarUInt / xyScale + ymin

      var dx = 0L
      var dy = 0L
      var ix = 0
      var iy = 1

      if (numParts > 1) {
        var sum = 0
        val numCoordSeq = 1 to numParts map (part => {
          val numCoord = if (part == numParts) {
            numPoints - sum
          } else {
            blob.getVarUInt.toInt
          }
          sum += numCoord
          numCoord
        })
        val polygons = numCoordSeq.map(numCoord => {
          val coordinates = getCoordinates(blob, numCoord, xyScale, xOrig, yOrig)
          geomFact.createLinearRing(coordinates)
        })
        val shell = polygons.head
        val holes = polygons.tail.toArray
        geomFact.createPolygon(shell, holes).toString()
      }
      else {
        geomFact.createPolygon(getCoordinates(blob, numPoints, xyScale, xOrig, yOrig)).toString()
      }
    } else {
      emptyPolygon
    }
  }

  def getCoordinates(byteBuffer: ByteBuffer, numCoordinates: Int,xyscale: Double,
                     xOrig: Double,
                     yOrig: Double) = {
    val coordinates = new Array[Coordinate](numCoordinates)
    0 until numCoordinates foreach (n => {
      dx += byteBuffer.getVarInt
      dy += byteBuffer.getVarInt
      val x = dx / xyscale + xOrig
      val y = dy / xyscale + yOrig
      coordinates(n) = new Coordinate(x, y)
    })
    coordinates
  }
}

object FieldMultiPart extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double
           ): FieldMultiPart = {
    new FieldMultiPart(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, true),
        StructField("ymin", DoubleType, true),
        StructField("xmax", DoubleType, true),
        StructField("ymax", DoubleType, true),
        StructField("parts", ArrayType(IntegerType), true),
        StructField("coords", ArrayType(DoubleType), true)
      )
      ), nullable, metadata), xOrig, yOrig, xyScale,xyTolerance)
  }
}
