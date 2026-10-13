package com.esri.gdb

case class GDBIndexRow(oid: Int, seek: Long)

object GDBIndexRow {
  val BySeek: java.util.Comparator[GDBIndexRow] = new java.util.Comparator[GDBIndexRow] {
    override def compare(a: GDBIndexRow, b: GDBIndexRow): Int = java.lang.Long.compare(a.seek, b.seek)
  }
}
