package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequence, GeometryFactory, PrecisionModel}

class FieldMultiPartZM(val field: StructField,
                       xOrig: Double,
                       yOrig: Double,
                       xyScale: Double,
                       zOrig: Double,
                       zScale: Double,
                       mOrig: Double,
                       mScale: Double,
                       xyTolerance: Double
                      ) extends FieldBytes {

  override type T = String

  @transient val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))

  @transient val emptyPolygon = geomFact.createPolygon(null.asInstanceOf[CoordinateSequence]).toString()

  override def readNull(): T = null.asInstanceOf[String]

  protected var dx = 0L
  protected var dy = 0L
  protected var dz = 0L

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
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
          val coordinates = getCoordinates(blob, numCoord, xyScale, xOrig, yOrig, zOrig, zScale)
          geomFact.createLinearRing(coordinates)
        })
        val shell = polygons.head
        val holes = polygons.tail.toArray
        geomFact.createPolygon(shell, holes).toString()
      }
      else {
        geomFact.createPolygon(getCoordinates(blob, numPoints, xyScale, xOrig, yOrig, zOrig, zScale)).toString()
      }
    } else {
      emptyPolygon
    }
  }
  def getCoordinates(byteBuffer: ByteBuffer, numCoordinates: Int, xyscale: Double,
                     xOrig: Double,
                     yOrig: Double,
                     zOrig: Double,
                     zScale: Double) = {
    val coordinates = new Array[Coordinate](numCoordinates)
    0 until numCoordinates foreach (n => {
      dx += byteBuffer.getVarInt
      dy += byteBuffer.getVarInt
      val x = dx / xyscale + xOrig
      val y = dy / xyscale + yOrig
      val z = dz / zScale + zOrig
      coordinates(n) = new Coordinate(x, y, z)
    })
    coordinates
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
            mScale: Double,
            xyTolerance: Double
           ): FieldMultiPartZM = {
    new FieldMultiPartZM(StructField(name,
      StructType(Seq(
        StructField("xmin", DoubleType, true),
        StructField("ymin", DoubleType, true),
        StructField("xmax", DoubleType, true),
        StructField("ymax", DoubleType, true),
        StructField("parts", ArrayType(IntegerType), true),
        StructField("coords", ArrayType(DoubleType), true))
      ), nullable, metadata), xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale, xyTolerance)
  }
}
