package com.esri.gdb

import java.io.File
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object GDBTable extends Serializable {

  def apply(conf: Configuration, path: String, name: String, wkid: Int): GDBTable = {
    val filename = StringBuilder.newBuilder.append(path).append(File.separator).append(name).append(".gdbtable").toString()
    // println(f"${Console.YELLOW}Opening '$filename'${Console.RESET}")
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
        // println(s"gdb ver=$i1")
        val geometryType = bb.get & 0x00FF
        val b2 = bb.get
        val b3 = bb.get
        val geometryProp = bb.get & 0x00FF // 0x40 for geometry with M, 0x80 for geometry with Z
        val numFields = bb.getShort & 0x7FFF
        // println(f"${Console.YELLOW}$name::maxRows = $maxRows geometryType = $geometryType%02X geometryProp = $geometryProp%02X numFields = $numFields${Console.RESET}")
        // val bb2 = dataBuffer.readBytes(numBytes)
        val fields = Array.fill[GDBField](numFields) {
          readField(bb, geometryType, geometryProp, wkid)
        }
        fields
      }
    }
    new GDBTable(dataBuffer, maxRows, fields)
  }

  private def readField(bb: ByteBuffer, geomType: Int, geomProp: Int, wkid: Int): GDBField = {
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
    // println(s"nameLen=$nameLen name=$name aliasLen=$aliasLen alias=$alias fieldType=$fieldType")
    fieldType match {
      case EsriFieldType.INT16 => toFieldInt16(bb, name, alias)
      case EsriFieldType.INT32 => toFieldInt32(bb, name, alias)
      case EsriFieldType.FLOAT32 => toFieldFloat32(bb, name, alias)
      case EsriFieldType.FLOAT64 => toFieldFloat64(bb, name, alias)
      case EsriFieldType.TIMESTAMP => toFieldTimestamp(bb, name, alias)
      case EsriFieldType.STRING => toFieldString(bb, name, alias)
      case EsriFieldType.OID => toFieldOID(bb, name, alias)
      case EsriFieldType.SHAPE => toFieldGeom(bb, name, alias, geomType, geomProp, wkid)
      case EsriFieldType.BINARY => toFieldBinary(bb, name, alias)
      case EsriFieldType.UUID | EsriFieldType.GUID => toFieldUUID(bb, name, alias)
      case EsriFieldType.XML => toFieldXML(bb, name, alias)
      case _ => throw new RuntimeException(s"Field $name with type $fieldType is not supported")
    }

  }

  private def readHeader(dataBuffer: DataBuffer, filename: String) = {
    try {
      val bb = dataBuffer.readBytes(40)
      val sig = bb.getInt // signature TODO - throw exception if not correct signature
      val numRows = bb.getInt
      val bodyBytes = bb.getUInt // number of packed bytes in the body
      val h3 = bb.getInt
      val h4 = bb.getInt
      val h5 = bb.getInt
      val fileBytes = bb.getUInt
      val h7 = bb.getInt
      val headBytes = bb.getInt // 40
      val h9 = bb.getInt
      // println(s"$sig $numRows $bodyBytes $h3 $h4 $h5 $fileBytes $h7 $headBytes $h9")
      (numRows, fileBytes - 40L)
    } catch {
      case t: Throwable =>
        println(s"${Console.RED}Exception (${t.toString}) thrown when reading '$filename'${Console.RESET}")
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
                          geometryProp: Int,
                          wkid: Int
                         ): GDBField = {
    bb.get // len
    val nullable = (bb.get & 1) == 1

    val srLen = bb.getShort
    val srChars = srLen / 2
    val stringBuilder = new StringBuilder(srChars)
    0 until srChars foreach (_ => stringBuilder.append(bb.getChar))
    val sr = stringBuilder.toString // not used :-(

    val zAndM = bb.get
    val (hasZ, hasM) = zAndM match {
      case 7 => (true, true)
      case 5 => (true, false)
      case _ => (false, false)
    }

    // println(s"geometryType=$geometryType zAndM=$zAndM hasZ=$hasZ hasM=$hasM geomProp=$geometryProp")

    val xOrig = bb.getDouble
    val yOrig = bb.getDouble
    val xyScale = bb.getDouble
    val mOrig = if (hasM) bb.getDouble else 0.0
    val mScale = if (hasM) bb.getDouble else 0.0
    val zOrig = if (hasZ) bb.getDouble else 0.0
    val zScale = if (hasZ) bb.getDouble else 0.0
    val xyTolerance = bb.getDouble
    val mTolerance = if (hasM) bb.getDouble else 0.0
    val zTolerance = if (hasZ) bb.getDouble else 0.0
    val xmin = bb.getDouble
    val ymin = bb.getDouble
    val xmax = bb.getDouble
    val ymax = bb.getDouble
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
      .putLong("wkid", wkid)
      .putDouble("xmin", xmin)
      .putDouble("ymin", ymin)
      .putDouble("xmax", xmax)
      .putDouble("ymax", ymax)
    /*
    .putBoolean("hasZ", hasZ)
    .putBoolean("hasM", hasM)
    .putDouble("xyTolerance", xyTolerance)

    if (hasZ) metadataBuilder.putDouble("zTolerance", zTolerance)
    if (hasM) metadataBuilder.putDouble("mTolerance", mTolerance)
    */

    val metadata = metadataBuilder.build()

    // TODO - more shapes and support Z and M
    geometryType match {
      case 1 =>
        geometryProp match {
          case 0x00 => FieldXY(name, nullable, metadata, xOrig, yOrig, xyScale)
          // case 0x40 => FieldPointMType(name, nullAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
          // case 0x80 => FieldPointZType(name, nullAllowed, xOrig, yOrig, zOrig, xyScale, zScale, metadata)
          // case _ => FieldPointZMType(name, nullAllowed, xOrig, yOrig, zOrig, mOrig, xyScale, zScale, mScale, metadata)
          case _ => throw new RuntimeException("Cannot parse (yet) point with M or Z value :-(")
        }
      case 3 =>
        geometryProp match {
          case 0x00 => FieldMultiPart(name, nullable, metadata, xOrig, yOrig, xyScale)
          // case 0x40 => FieldPolylineMType(name, nullAllowed, xOrig, yOrig, mOrig, xyScale, mScale, metadata)
          case _ => throw new RuntimeException("Cannot parse (yet) polylines with Z value :-(")
        }
      case 4 | 5 =>
        FieldMultiPart(name, nullable, metadata, xOrig, yOrig, xyScale)
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
