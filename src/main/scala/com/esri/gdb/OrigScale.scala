package com.esri.gdb

case class OrigScale(xOrig: Double,
                     yOrig: Double,
                     xyScale: Double,
                     zOrig: Double,
                     zScale: Double,
                     mOrig: Double,
                     mScale: Double) {
  @inline
  def calcM(dm: Long): Double = if (dm == -1) Double.NaN else dm / mScale + mOrig
}
