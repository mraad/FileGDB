package com.esri.gdb

case class NameIndex(name: String, index: Int) extends Ordered[NameIndex] {

  def toTableName: String = "a%08x".format(index)

  override def compare(that: NameIndex): Int = {
    this.name compareTo that.name
  }

}
