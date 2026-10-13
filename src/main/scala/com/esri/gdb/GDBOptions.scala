package com.esri.gdb

object GDBOptions extends Serializable {
  val PATH = "path"
  val NAME = "name"
  val NUM_PARTITIONS = "numPartitions"
  // "true" reads each partition in .gdbtable file order instead of OBJECTID order. See README.
  val SORT_BY_OFFSET = "sortByOffset"
  @deprecated
  val WKID = "wkid"
}
