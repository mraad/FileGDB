package com.esri.gdb

import java.nio.ByteBuffer

import org.apache.spark.sql.types._

import com.vividsolutions.jts.geom.{Coordinate, CoordinateSequence, GeometryFactory, PrecisionModel}

class FieldXY(val field: StructField,
              xOrig: Double,
              yOrig: Double,
              xyScale: Double,
              xyTolerance: Double
             ) extends FieldBytes {

  override type T = String

  @transient val geomFact = new GeometryFactory(new PrecisionModel(1.0 / xyTolerance))

  override def readNull(): T = null.asInstanceOf[String]

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    val blob = getByteBuffer(byteBuffer)

    blob.getVarUInt // geomType

    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    geomFact.createPoint(new Coordinate(x, y)).toString()
  }
}

object FieldXY extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            xyTolerance: Double
           ): FieldXY = {
    new FieldXY(StructField(name,
      StructType(Seq(StructField("x", DoubleType, true), StructField("y", DoubleType, true))
      ), nullable, metadata), xOrig, yOrig, xyScale, xyTolerance)
  }
}
