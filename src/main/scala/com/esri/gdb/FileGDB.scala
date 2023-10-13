package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType


class FileGDB(index: GDBIndex, table: GDBTable) extends AutoCloseable with Serializable {
  def schema(): StructType = table.schema

  def rows(numRowsToRead: Int = -1, startAtRow: Int = 0): Iterator[Row] = table.rows(index, numRowsToRead, startAtRow)

  override def close(): Unit = {
    table.close()
    index.close()
  }
}

object FileGDB extends Serializable {

  // private val logger = LoggerFactory.getLogger(getClass)

  def listTables(pathName: String,
                 conf: Configuration
                ): Array[NameIndex] = {
    val gdbIndex = GDBIndex(conf, pathName, "a00000001")
    try {
      val gdbTable = GDBTable(conf, pathName, "a00000001")
      try {
        val indexID = gdbTable.schema.fieldIndex("ID")
        val indexName = gdbTable.schema.fieldIndex("Name")
        // val indexFileFormat = gdbTable.schema.fieldIndex("FileFormat")
        gdbTable
          .rows(gdbIndex, gdbIndex.maxRows)
          .map(row => {
            val id = row.getInt(indexID)
            val name = row.getString(indexName)
            // val fileFormat = row.getInt(indexFileFormat)
            // logger.debug(s"listTables::id=$id name=$name")
            NameIndex(name, id)
          })
          .toArray
      } finally {
        gdbTable.close()
      }
    } finally {
      gdbIndex.close()
    }
  }

  def listTableNames(pathName: String,
                     conf: Configuration
                    ): Array[String] = listTables(pathName, conf).map(_.name)

  def findTable(pathName: String,
                tableName: String,
                conf: Configuration
               ): Option[NameIndex] = {
    listTables(pathName, conf).find(_.name == tableName)
  }

  def schema(pathName: String,
             tableName: String,
             conf: Configuration
            ): Option[StructType] = {
    findTable(pathName, tableName, conf) match {
      case Some(catRow) =>
        val table = GDBTable(conf, pathName, catRow.toTableName)
        try {
          Some(table.schema)
        }
        finally {
          table.close()
        }
      case _ => None
    }
  }

  def rows(pathName: String,
           tableName: String,
           conf: Configuration
          ): Array[Row] = {
    findTable(pathName, tableName, conf) match {
      case Some(catRow) => {
        val internalName = catRow.toTableName
        val table = GDBTable(conf, pathName, internalName)
        try {
          val index = GDBIndex(conf, pathName, internalName)
          try {
            table.rows(index).toArray
          } finally {
            index.close()
          }
        }
        finally {
          table.close()
        }
      }
      case _ => Array.empty
    }
  }

  @deprecated
  def rows(pathName: String,
           tableName: String,
           func: Row => Boolean,
           conf: Configuration
          ): Boolean = {
    findTable(pathName, tableName, conf) match {
      case Some(catRow) => {
        val internalName = catRow.toTableName
        val table = GDBTable(conf, pathName, internalName)
        try {
          val index = GDBIndex(conf, pathName, internalName)
          try {
            table.rows(index).forall(func(_))
          } finally {
            index.close()
          }
        }
        finally {
          table.close()
        }
      }
      case _ => false
    }
  }

  def apply(pathName: String, tableName: String, conf: Configuration = new Configuration()): Option[FileGDB] = {
    findTable(pathName, tableName, conf) match {
      case Some(catRow) => {
        val internalName = catRow.toTableName
        val index = GDBIndex(conf, pathName, internalName)
        val table = GDBTable(conf, pathName, internalName)
        Some(new FileGDB(index, table))
      }
      case _ => None
    }
  }
}
