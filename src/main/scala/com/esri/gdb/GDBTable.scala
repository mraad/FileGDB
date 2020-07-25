package com.esri.gdb

import java.io.File
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object GDBTable extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(conf: Configuration, path: String, name: String): GDBTable = {
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtable").toString()
    if (logger.isDebugEnabled) {
      logger.debug(f"Opening '$filename'")
    }
    val hdfsPath = new Path(filename)
    val dataBuffer = DataBuffer(hdfsPath.getFileSystem(conf).open(hdfsPath))
    val (maxRows, bodyBytes) = readHeader(dataBuffer, filename)
    val fields = (maxRows, bodyBytes) match {
      case (0, 0L) =>
        Array.empty[GDBField]
      case _ => {
        val bb = dataBuffer.readBytes((bodyBytes & 0x7FFFFFFF).toInt) // Hack for now.
        val numBytes = bb.getInt
        // println(s"bodyByte=$bodyBytes numBytes=$numBytes")
        val gdbVer = bb.getInt // Seems to be 3 for FGDB 9.X files and 4 for FGDB 10.X files
        if (logger.isDebugEnabled) {
          logger.debug(s"gdb ver=$gdbVer")
        }
        // Read next 32 bits as 4 individual bytes.
        val geometryType = bb.get & 0x00FF
        val b2 = bb.get
        val b3 = bb.get
        val geometryProp = bb.get & 0x00FF // 0x40 for geometry with M, 0x80 for geometry with Z
        val numFields = bb.getShort & 0x7FFF
        if (logger.isDebugEnabled) {
          logger.debug(f"$name::maxRows=$maxRows geometryType=$geometryType%02X geometryProp=$geometryProp%02X numFields=$numFields")
        }
        // val bb2 = dataBuffer.readBytes(numBytes)
        val fields = Array.fill[GDBField](numFields) {
          readField(bb, geometryType, geometryProp)
        }
        fields
      }
    }
    new GDBTable(dataBuffer, maxRows, fields)
  }

  private def readField(bb: ByteBuffer,
                        geomType: Int,
                        geomProp: Int
                       ): GDBField = {
    val nameLen = bb.get
    val nameBuilder = new StringBuilder(nameLen)
    var n = 0
    while (n < nameLen) {
      nameBuilder.append(bb.getChar)
      n += 1
    }
    val name = nameBuilder.toString

    val aliasLen = bb.get
    val aliasBuilder = new StringBuilder(aliasLen)
    n = 0
    while (n < aliasLen) {
      aliasBuilder.append(bb.getChar)
      n += 1
    }
    val alias = if (aliasLen > 0) aliasBuilder.toString else name
    val fieldType = bb.get
    if (logger.isDebugEnabled) {
      logger.debug(s"nameLen=$nameLen name=$name aliasLen=$aliasLen alias=$alias fieldType=$fieldType")
    }
    fieldType match {
      case EsriFieldType.INT16 => toFieldInt16(bb, name, alias)
      case EsriFieldType.INT32 => toFieldInt32(bb, name, alias)
      case EsriFieldType.FLOAT32 => toFieldFloat32(bb, name, alias)
      case EsriFieldType.FLOAT64 => toFieldFloat64(bb, name, alias)
      case EsriFieldType.TIMESTAMP => toFieldTimestamp(bb, name, alias)
      case EsriFieldType.STRING => toFieldString(bb, name, alias)
      case EsriFieldType.OID => toFieldOID(bb, name, alias)
      case EsriFieldType.SHAPE => toFieldGeom(bb, name, alias, geomType, geomProp)
      case EsriFieldType.BINARY => toFieldBinary(bb, name, alias)
      case EsriFieldType.UUID | EsriFieldType.GUID => toFieldUUID(bb, name, alias)
      case EsriFieldType.XML => toFieldXML(bb, name, alias)
      case _ => throw new RuntimeException(s"Field $name with type $fieldType is not supported")
    }

  }

  private def readHeader(dataBuffer: DataBuffer, filename: String) = {
    try {
      val bb = dataBuffer.readBytes(40)
      val sig = bb.getInt // signature
      if (sig == 3) {
        val numRows = bb.getInt
        val bodyBytes = bb.getUInt // number of packed bytes in the body
        val h3 = bb.getInt
        val h4 = bb.getInt
        val h5 = bb.getInt
        val fileBytes = bb.getUInt
        val h7 = bb.getInt
        val headBytes = bb.getInt // 40
        val h9 = bb.getInt
        // println(s"sig=$sig numRows=$numRows bodyBytes=$bodyBytes $h3 $h4 $h5 fileBytes=$fileBytes $h7 headBytes=$headBytes $h9")
        (numRows, fileBytes - 40L)
      }
      else {
        logger.warn(f"${Console.RED}Invalid signature for '$filename'.  Is the table compressed ?${Console.RESET}")
        (0, 0L)
      }
    } catch {
      case t: Throwable =>
        logger.error(filename, t)
        (0, 0L)
    }
  }

  private def toFieldFloat32(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldFloat32(StructField(name, FloatType, nullable, metadata))
  }

  private def toFieldFloat64(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldFloat64(StructField(name, DoubleType, nullable, metadata))
  }

  private def toFieldInt16(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldInt16(StructField(name, ShortType, nullable, metadata))
  }

  private def toFieldInt32(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldInt32(StructField(name, IntegerType, nullable, metadata))
  }

  private def toFieldBinary(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldBinary(StructField(name, BinaryType, nullable, metadata))
  }

  private def toFieldUUID(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldUUID(StructField(name, StringType, nullable, metadata))
  }

  private def toFieldXML(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldString(StructField(name, StringType, nullable, metadata))
  }

  private def toFieldString(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.getInt
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldString(StructField(name, StringType, nullable, metadata))
  }

  private def toFieldTimestamp(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt
    while (lenDefVal > 0) {
      bb.get()
      lenDefVal -= 1
    }
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldTimestamp(StructField(name, TimestampType, nullable, metadata))
  }

  /*
  private def toFieldMillis(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    bb.get // mask
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldMillis(StructField(name, LongType, nullable, metadata))
  }
  */

  private def toFieldOID(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    val metadata = new MetadataBuilder()
      .putString("alias", alias)
      .putLong("len", len)
      .build()
    new FieldOID(StructField(name, IntegerType, nullable, metadata))
  }

  private def toFieldGeom(bb: ByteBuffer,
                          name: String,
                          alias: String,
                          geometryType: Int,
                          geometryProp: Int
                         ): FieldBytes = {
    bb.get // len
    val nullable = (bb.get & 1) == 1

    // Spatial Reference WKT
    val srLen = bb.getShort
    val srChars = srLen / 2
    val stringBuilder = new StringBuilder(srChars)
    0 until srChars foreach (_ => stringBuilder.append(bb.getChar))
    val wkt = stringBuilder.toString

    val getZM = bb.getUByte
    val (getZ, getM) = getZM match {
      case 7 => (true, true)
      case 5 => (true, false)
      case _ => (false, false)
    }
    val (hasZ, hasM) = geometryProp & 0x00C0 match {
      case 0x00C0 => (true, true)
      case 0x0080 => (true, false)
      case 0x0040 => (false, true)
      case _ => (false, false)
    }

    if (logger.isDebugEnabled)
      logger.debug(f"geomType=$geometryType%X geomProp=$geometryProp%X getZ=$getZ getM=$getM hasZ=$hasZ hasM=$hasM")

    val xOrig = bb.getDouble
    val yOrig = bb.getDouble
    val xyScale = bb.getDouble
    val mOrig = if (getM) bb.getDouble else 0.0
    val mScale = if (getM) bb.getDouble else 0.0
    val zOrig = if (getZ) bb.getDouble else 0.0
    val zScale = if (getZ) bb.getDouble else 0.0
    val xyTolerance = bb.getDouble
    val mTolerance = if (getM) bb.getDouble else 0.0
    val zTolerance = if (getZ) bb.getDouble else 0.0
    val xmin = bb.getDouble
    val ymin = bb.getDouble
    val xmax = bb.getDouble
    val ymax = bb.getDouble
//    val zmin = if (getZ) bb.getDouble else 0.0
//    val zmax = if (getZ) bb.getDouble else 0.0
//    val mmin = if (getM) bb.getDouble else 0.0
//    val mmax = if (getM) bb.getDouble else 0.0
    // Not sure what does !!
    val numes = new ArrayBuffer[Double]()
    var cont = true
    while (cont) {
      val pos = bb.position
      val m1 = bb.get
      val m2 = bb.get
      val m3 = bb.get
      val m4 = bb.get
      val m5 = bb.get
      if (m1 == 0 && m2 > 0 && m3 == 0 && m4 == 0 && m5 == 0) {
        0 until m2 foreach (_ => numes += bb.getDouble)
        cont = false
      }
      else {
        bb.position(pos)
        numes += bb.getDouble
      }
    }

    val metadataBuilder = new MetadataBuilder()
      .putString("alias", alias)
      .putString("srsWKT", wkt)
      .putDouble("xmin", xmin)
      .putDouble("ymin", ymin)
      .putDouble("xmax", xmax)
      .putDouble("ymax", ymax)
      .putLong("geomType", geometryType)
      .putBoolean("hasZ", hasZ)
      .putBoolean("hasM", hasM)
    val metadata = metadataBuilder.build()
    if (logger.isDebugEnabled) {
      logger.debug(metadata.json)
    }

    // TODO - more shapes and support Z and M
    geometryType match {
      case 1 => // Point
        geometryProp match {
          case 0x00 => FieldXY(name, nullable, metadata, xOrig, yOrig, xyScale, xyTolerance)
          // case 0x40 => FieldPointMType(name, nullAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
          // case 0x80 => FieldPointZType(name, nullAllowed, xOrig, yOrig, zOrig, xyScale, zScale, metadata)
          // case _ => FieldPointZMType(name, nullAllowed, xOrig, yOrig, zOrig, mOrig, xyScale, zScale, mScale, metadata)
          case _ => throw new RuntimeException("Cannot parse (yet) point with M or Z value :-(")
        }
      case 3 => // Polyline
        geometryProp match {
          case 0x00 => FieldMultiPart(name, nullable, metadata, xOrig, yOrig, xyScale, xyTolerance)
          case 0xC0 => FieldMultiPartZM(name, nullable, metadata, xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale, xyTolerance)
          case _ => throw new RuntimeException(f"Cannot parse (yet) polyline with geometryProp value of $geometryProp%X :-(")
        }
      case 4 | 5 => // Polygon
        geometryProp match {
          case 0x00 => FieldMultiPart(name, nullable, metadata, xOrig, yOrig, xyScale, xyTolerance)
          case 0xC0 => FieldMultiPartZM(name, nullable, metadata, xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale, xyTolerance)
          case _ => throw new RuntimeException(f"Cannot parse (yet) polygons with geometryProp value of $geometryProp%X :-(")
        }
      case _ =>
        new FieldGeomNoop(StructField(name, StringType, nullable, metadata))
    }
  }

}

class GDBTable(dataBuffer: DataBuffer, val maxRows: Int, val fields: Array[GDBField]) extends AutoCloseable with Serializable {

  val schema: StructType = StructType(fields.map(_.field))

  def rows(index: GDBIndex, numRowsToRead: Int = -1, startAtRow: Int = 0): Iterator[Row] = {
    val numRows = if (numRowsToRead < 0) maxRows else numRowsToRead
    new GDBTableIterator(index.indicies(numRows, startAtRow), dataBuffer, fields, schema)
  }

  override def close(): Unit = {
    dataBuffer.close()
  }
}
