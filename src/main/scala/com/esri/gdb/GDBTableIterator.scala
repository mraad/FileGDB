package com.esri.gdb

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 */
class GDBTableIterator(indexIter: Iterator[GDBIndexRow],
                       dataBuffer: DataBuffer,
                       fields: Array[GDBField],
                       schema: StructType
                      ) extends Iterator[Row] with Serializable {

  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val numFieldsWithNullAllowed = fields.count(_.nullable())
  private val nullValueMasks = new Array[Byte]((numFieldsWithNullAllowed / 8.0).ceil.toInt)

  def hasNext(): Boolean = {
    indexIter.hasNext
  }

  def next(): Row = {
    val index = indexIter.next()
    val numBytes = dataBuffer.seek(index.seek).readBytes(4).getInt
    val byteBuffer = dataBuffer.readBytes(numBytes)
    var n = 0
    while (n < nullValueMasks.length) {
      nullValueMasks(n) = byteBuffer.get
      n += 1
    }
    var bit = 0
    val values: Array[Any] = try {
      fields.map(field => {
        if (field.nullable()) {
          val i = bit >> 3
          val m = 1 << (bit & 7)
          bit += 1
          if ((nullValueMasks(i) & m) == 0) {
            field.readValue(byteBuffer, index.oid)
          }
          else {
            field.readNull()
          }
        } else {
          field.readValue(byteBuffer, index.oid)
        }
      })
    } catch {
      case t: Throwable =>
        logger.error(s"OBJECTID=${index.oid}, numBytes=$numBytes", t)
        fields.map(_.readNull())
    }
    new GenericRowWithSchema(values, schema)
  }
}
