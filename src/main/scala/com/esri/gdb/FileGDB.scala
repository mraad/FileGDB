package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


class FileGDB(index: GDBIndex, table: GDBTable) extends AutoCloseable with Serializable {
  def schema(): StructType = {
    table.schema
  }

  def rows(numRowsToRead: Int = -1, startAtRow: Int = 0): Iterator[Row] = table.rows(index, numRowsToRead, startAtRow)

  override def close(): Unit = {
    table.close()
    index.close()
  }
}

object FileGDB extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)

  def listTables(pathName: String,
                 conf: Configuration
                ): Array[NameIndex] = {
    val gdbIndex = GDBIndex(conf, pathName, "a00000001")
    try {
      val gdbTable = GDBTable(conf, pathName, "a00000001")
      try {
        val indexID = gdbTable.schema.fieldIndex("ID")
        val indexName = gdbTable.schema.fieldIndex("Name")
        val indexFileFormat = gdbTable.schema.fieldIndex("FileFormat")
        gdbTable
          .rows(gdbIndex, gdbTable.maxRows)
          .map(row => {
            val id = row.getInt(indexID)
            val name = row.getString(indexName)
            val fileFormat = row.getInt(indexFileFormat)
            if (logger.isDebugEnabled) {
              logger.debug(s"listTables::id=$id name=$name fileFormat=$fileFormat")
            }
            NameIndex(name, id)
          })
          .filterNot(row => {
            row.name.startsWith("GDB_")
          })
          .toArray
      } finally {
        gdbTable.close()
      }
    } finally {
      gdbIndex.close()
    }
  }

  // So it can be used from PySpark
  // def listTables(pathName: String): Array[NameIndex] = listTables(pathName, new Configuration()): Array[NameIndex]

  def listTableNames(pathName: String,
                     conf: Configuration
                    ): Array[String] = listTables(pathName, conf).map(_.name)

  def pyListTableNames(pathName: String) = {
    val spark = SparkSession.builder.getOrCreate()
    listTableNames(pathName, spark.sparkContext.hadoopConfiguration)
  }

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
      case Some(catRow) => {
        val table = GDBTable(conf, pathName, catRow.toTableName)
        try {
          Some(table.schema)
        }
        finally {
          table.close()
        }
      }
      case _ => None
    }
  }

  // So it can be user by PySpark
  //  def schema(pathName: String, tableName: String): StructType = {
  //    schema(pathName, tableName, new Configuration()) match {
  //      case Some(schema) => schema
  //      case _ => StructType(Seq.empty)
  //    }
  //  }

  //  def rows(pathName: String, tableName: String): Array[Row] = {
  //    rows(pathName, tableName, new Configuration())
  //  }

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
