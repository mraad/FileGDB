package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskCompletionListener
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import scala.collection.mutable

case class GDBTableHeader(numFeatures: Int, largestSize: Int, fields: Array[GDBField])

object GDBTable extends Serializable {
  private val logger = LoggerFactory.getLogger(getClass)
  private val map = mutable.Map.empty[String, GDBTableHeader]

  def apply(conf: Configuration, path: String, name: String, context: Option[TaskContext] = None): GDBTable = {
    val filename = StringBuilder.newBuilder.append(path).append("/").append(name).append(".gdbtable").toString()
    val hdfsPath = new Path(filename)
    val dataInput = hdfsPath.getFileSystem(conf).open(hdfsPath)
    val dataBuffer = DataBuffer(dataInput)

    def readTableHeader: GDBTableHeader = {
      logger.debug(f"Cache $filename...")
      val sig = dataBuffer.getInt()
      if (sig == 3) {
        val numFeatures = dataBuffer.getInt()
        val largestSize = dataBuffer.getInt()
        dataBuffer.resize(largestSize)
        dataBuffer.seek(32L)
        val headerOffset = dataBuffer.getLong()
        dataBuffer.seek(headerOffset)
        val headerLength = dataBuffer.getInt()
        val bb = dataBuffer.readBytes(headerLength)
        val gdbVer = bb.getInt
        val geometryType = bb.get & 0x00FF
        val b2 = bb.get
        val b3 = bb.get
        val geometryProp = bb.get & 0x00FF
        val hasZ = (geometryProp & (1 << 7)) != 0
        val hasM = (geometryProp & (1 << 6)) != 0
        val numFields = bb.getShort & 0x7FFF
        if (logger.isDebugEnabled()) {
          logger.debug(s"numFeatures=$numFeatures")
          val geometryTypeText = geometryType match {
            case 1 => "Point"
            case 2 => "MultiPoint"
            case 3 => "Polyline"
            case 4 => "Polygon"
            case 9 => "Multipatch"
            case _ => "Unknown"
          }
          logger.debug(s"geometryType=$geometryType ($geometryTypeText)")
          logger.debug(s"geometryProp=${geometryProp.toHexString}")
          logger.debug(s"hasZ=$hasZ hasM=$hasM")
          logger.debug(s"numFields=$numFields")
        }
        val fields = Array.fill[GDBField](numFields) {
          readField(bb, geometryType, geometryProp)
        }
        GDBTableHeader(numFeatures, largestSize, fields)
      }
      else {
        logger.warn(f"${Console.RED}Invalid signature for '$filename'.  Is the table compressed ?${Console.RESET}")
        GDBTableHeader(0, 1024, Array.empty[GDBField])
      }
    }

    val tableHeader = map.getOrElseUpdate(filename, readTableHeader)
    new GDBTable(dataBuffer, tableHeader, context)
  }

  private def readField(bb: ByteBuffer,
                        geomType: Int,
                        geomProp: Int
                       ): GDBField = {
    val nameLen = bb.get & 0x00FF
    val nameBuilder = new StringBuilder(nameLen)
    var n = 0
    while (n < nameLen) {
      nameBuilder.append(bb.getChar)
      n += 1
    }
    val name = nameBuilder.toString

    val aliasLen = bb.get & 0x00FF
    val aliasBuilder = new StringBuilder(aliasLen)
    n = 0
    while (n < aliasLen) {
      aliasBuilder.append(bb.getChar)
      n += 1
    }
    val alias = if (aliasLen > 0) aliasBuilder.toString else name
    val fieldType = bb.get
    // logger.debug(s"nameLen=$nameLen name=$name aliasLen=$aliasLen alias=$alias fieldType=$fieldType")
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

  private def toFieldFloat32(bb: ByteBuffer, name: String, alias: String): GDBField = {
    val len = bb.get
    val nullable = (bb.get & 1) == 1
    var lenDefVal = bb.getVarUInt()
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
    var lenDefVal = bb.getVarUInt()
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
    var lenDefVal = bb.getVarUInt()
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
    var lenDefVal = bb.getVarUInt()
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
    var lenDefVal = bb.getVarUInt()
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
    var lenDefVal = bb.getVarUInt()
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

    val getZM = bb.getUByte()
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
    logger.debug(s"xyTolerance=$xyTolerance mTolerance=$mTolerance zTolerance=$zTolerance")
    val xmin = bb.getDouble
    val ymin = bb.getDouble
    val xmax = bb.getDouble
    val ymax = bb.getDouble
    val zmin = if (hasZ) bb.getDouble else 0.0
    val zmax = if (hasZ) bb.getDouble else 0.0
    val mmin = if (hasM) bb.getDouble else 0.0
    val mmax = if (hasM) bb.getDouble else 0.0
    logger.debug(s"x=$xmin,$xmax y=$ymin,$ymax z=$zmin,$zmax m=$mmin,$mmax")
    val byteAtZero = bb.get()
    val gridMax = bb.getInt()
    logger.debug(s"griMax=$gridMax")
    var gridInd = 0
    while (gridInd < gridMax) {
      bb.getDouble()
      gridInd += 1
    }

    val metadataBuilder = new MetadataBuilder()
      .putString("alias", alias)
      .putString("srsWKT", wkt)
      .putDouble("xmin", xmin)
      .putDouble("xmax", xmax)
      .putDouble("ymin", ymin)
      .putDouble("ymax", ymax)
      .putDouble("zmin", zmin)
      .putDouble("zmax", zmax)
      .putDouble("mmin", mmin)
      .putDouble("mmax", mmax)
      .putLong("geomType", geometryType)
      .putBoolean("hasZ", hasZ)
      .putBoolean("hasM", hasM)
    val metadata = metadataBuilder.build()
    // logger.debug(metadata.json)

    val origScale = OrigScale(xOrig, yOrig, xyScale, zOrig, zScale, mOrig, mScale)
    geometryType match {
      case 1 => // Point
        geometryProp match {
          case 0x00 => FieldXY(name, nullable, metadata, origScale)
          case 0x40 => FieldXYM(name, nullable, metadata, origScale)
          case 0x80 => FieldXYZ(name, nullable, metadata, origScale)
          case 0xC0 => FieldXYZM(name, nullable, metadata, origScale)
          case _ => throw new RuntimeException(f"Cannot parse (yet) point with geometryProp value of $geometryProp%X :-(")
        }
      case 2 => // Multipoint
        geometryProp match {
          case 0x00 => FieldMultiPoint(name, nullable, metadata, origScale)
          case 0x40 => FieldMultiPointM(name, nullable, metadata, origScale)
          case 0x80 => FieldMultiPointZ(name, nullable, metadata, origScale)
          case 0xC0 => FieldMultiPointZM(name, nullable, metadata, origScale)
          case _ => throw new RuntimeException(f"Cannot parse (yet) multipoint with geometryProp value of $geometryProp%X :-(")
        }
      case 3 => // Polyline
        geometryProp match {
          case 0x00 => FieldMultiPart(name, nullable, metadata, origScale)
          case 0x40 => FieldMultiPartM(name, nullable, metadata, origScale)
          case 0x80 => FieldMultiPartZ(name, nullable, metadata, origScale)
          case 0xC0 => FieldMultiPartZM(name, nullable, metadata, origScale)
          case _ => throw new RuntimeException(f"Cannot parse (yet) polyline with geometryProp value of $geometryProp%X :-(")
        }
      case 4 | 5 => // Polygon
        geometryProp match {
          case 0x00 => FieldMultiPart(name, nullable, metadata, origScale)
          case 0x40 => FieldMultiPartM(name, nullable, metadata, origScale)
          case 0x80 => FieldMultiPartZ(name, nullable, metadata, origScale)
          case 0xC0 => FieldMultiPartZM(name, nullable, metadata, origScale)
          case _ => throw new RuntimeException(f"Cannot parse (yet) polygons with geometryProp value of $geometryProp%X :-(")
        }
      case _ =>
        new FieldGeomNoop(StructField(name, StringType, nullable, metadata))
    }
  }
}

class GDBTable(dataBuffer: DataBuffer,
               header: GDBTableHeader,
               context: Option[TaskContext]
              ) extends TaskCompletionListener with AutoCloseable with Serializable {

  val schema: StructType = StructType(header.fields.map(_.field))

  def rows(index: GDBIndex, numRows: Int = header.numFeatures, startAtRow: Int = 0): Iterator[Row] = {
    val fieldsCopy = header.fields.map(_.copy())
    val iterator = new GDBTableIterator(index.indices(numRows, startAtRow), dataBuffer, fieldsCopy)
    if (context.isDefined) {
      context.get.addTaskCompletionListener(iterator)
    }
    iterator
  }

  override def close(): Unit = {
    dataBuffer.close()
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    dataBuffer.close()
  }
}
