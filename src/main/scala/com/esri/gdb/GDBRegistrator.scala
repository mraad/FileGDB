package com.esri.gdb

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class GDBRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {

    kryo.register(classOf[DataBuffer])
    kryo.register(classOf[DefaultSource])
    kryo.register(classOf[FieldMultiPart])
    kryo.register(classOf[FieldMultiPartZ])
    kryo.register(classOf[FieldMultiPartZM])
    kryo.register(classOf[FieldMultiPoint])
    kryo.register(classOf[FieldMultiPointZ])
    kryo.register(classOf[FieldMultiPointZM])
    kryo.register(classOf[FieldXY])
    kryo.register(classOf[FieldXYM])
    kryo.register(classOf[FieldXYZ])
    kryo.register(classOf[FieldXYZM])
    kryo.register(classOf[FileGDB])
    kryo.register(classOf[FileGDB])
    kryo.register(classOf[GDBField])
    kryo.register(classOf[GDBIndex])
    kryo.register(classOf[GDBIndexRow])
    kryo.register(classOf[GDBRDD])
    kryo.register(classOf[GDBRDD])
    kryo.register(classOf[GDBRelation])
    kryo.register(classOf[GDBTable])
    kryo.register(classOf[GDBTableIterator])
    kryo.register(classOf[NameIndex])
  }
}
