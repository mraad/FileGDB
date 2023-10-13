package com.esri.gdb

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Ignore}


@Ignore
class ReadSuite extends AnyFlatSpec with BeforeAndAfterAll {

  it should "read gdb" in {
    val conf = new Configuration()
    val filename = "data/Test.gdb/a00000009.gdbtable"
    val hdfsPath = new Path(filename)
    val dataBuffer = new DataBuffer(hdfsPath.getFileSystem(conf).open(hdfsPath))
    try {
      dataBuffer.seek(4L)
      val numFeatures = dataBuffer.getInt
      println(s"numFeatures=$numFeatures")
      val largestSize = dataBuffer.getInt
      println(s"largestSize=$largestSize")
      dataBuffer.resize(largestSize)
      dataBuffer.seek(32L)
      val headerOffset = dataBuffer.getLong
      println(s"headerOffset=$headerOffset")
      dataBuffer.seek(headerOffset)
      val headerLength = dataBuffer.getInt()
      println(s"headerLength=$headerLength")
      val bb = dataBuffer.readBytes(headerLength)
      val gdbVer = bb.getInt
      println(s"gdbVer=$gdbVer")
      val geometryType = bb.get & 0x00FF
      println(s"geometryType=$geometryType")
      val b2 = bb.get
      val b3 = bb.get
      val geometryProp = bb.get & 0x00FF
      val hasZ = (geometryProp & (1 << 7)) != 0
      val hasM = (geometryProp & (1 << 6)) != 0
      println(s"hasZ=$hasZ hasM=$hasM")
      val numFields = bb.getShort & 0x7FFF
      println(s"numFields=$numFields")
    } finally {
      dataBuffer.close()
    }
  }

}
