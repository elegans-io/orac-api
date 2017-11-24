package io.elegans.orac.services

object RandomNumbers {
  val random = scala.util.Random

  def getIntPos(): Int = {
    math.abs(random.nextInt)
  }

  def getFloatPos(): Float = {
    math.abs(random.nextFloat)
  }

  def getDoublePos(): Double = {
    math.abs(random.nextDouble)
  }

  def getInt(): Int = {
    random.nextInt
  }

  def getFloat(): Float = {
    random.nextFloat
  }

  def getDouble(): Double = {
    random.nextDouble
  }

  def getLong(): Long = {
    random.nextLong()
  }

  def getString(size: Int): String = {
    random.alphanumeric.take(size).mkString
  }
}
