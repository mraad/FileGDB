package com.esri.gdb

import java.nio.ByteBuffer

import com.esri.core.geometry.{OperatorExportToJson, Point, SpatialReference}
import org.apache.spark.sql.types.{Metadata, StringType, StructField}

class FieldPoint(val field: StructField,
                 xOrig: Double,
                 yOrig: Double,
                 xyScale: Double,
                 wkt: String
                ) extends FieldBytes {

  override type T = String

  val oper = OperatorExportToJson.local
  val spatialReference = SpatialReference.create(wkt)

  override def readValue(byteBuffer: ByteBuffer, oid: Int): String = {
    val blob = getByteBuffer(byteBuffer)

    blob.getVarUInt // geomType

    val vx = blob.getVarUInt()
    val vy = blob.getVarUInt()
    val x = (vx - 1.0) / xyScale + xOrig
    val y = (vy - 1.0) / xyScale + yOrig

    oper.execute(spatialReference, new Point(x, y))
  }
}

object FieldPoint extends Serializable {
  def apply(name: String,
            nullable: Boolean,
            metadata: Metadata,
            xOrig: Double,
            yOrig: Double,
            xyScale: Double,
            wkt: String) = new FieldPoint(StructField(name, StringType, nullable, metadata), xOrig, yOrig, xyScale, wkt)
}
