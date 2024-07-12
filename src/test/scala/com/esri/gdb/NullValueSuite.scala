package com.esri.gdb

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class NullValueSuite extends AnyFlatSpec with BeforeAndAfterAll {
  it should "read null value" in {
    // float
    val scalaFloat = null.asInstanceOf[Float]
    assert(scalaFloat == 0.0)
    val javaFloat = null.asInstanceOf[java.lang.Float]
    assert(javaFloat == null)

    // double
    val scalaDouble = null.asInstanceOf[Double]
    assert(scalaDouble == 0.0)
    val javaDouble = null.asInstanceOf[java.lang.Double]
    assert(javaDouble == null)

    // short
    val scalaShort = null.asInstanceOf[Short]
    assert(scalaShort == 0)
    val javaShort = null.asInstanceOf[java.lang.Short]
    assert(javaShort == null)

    // int
    val scalaInt = null.asInstanceOf[Int]
    assert(scalaInt == 0)
    val javaInt = null.asInstanceOf[java.lang.Integer]
    assert(javaInt == null)
  }
}
